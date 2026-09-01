# eventual-cache — implementační plán

> **Status: hotovo.** Knihovna je naimplementovaná a leží vedle tohohle dokumentu
> (`README.md` + zdrojáky). Autoritativní popis chování je v `README.md`; tenhle
> dokument zůstává jako záznam rozhodnutí a měření, na kterých návrh stojí.
>
> Co se oproti plánu ještě změnilo:
>
> - **`ReloadInterval` a `MinIDsRatio` vypadly.** Periodické stažení všech ID plus
>   invalidace ze zdroje pokryjí obojí; kolik položek se dorovnalo se jen loguje
>   a měří (`sync_added_count`, `sync_removed_count`).
> - **`state[T]` jako samostatný snapshot vypadl.** Měření ukázalo, že extra
>   pointerový skok (mapa → entry → state → hodnota) při 200 tis. položkách a
>   rozházené haldě **zdvojnásobí cenu `Get`** (226 ns vs 120 ns). Hodnota je
>   proto přímo v `entry` a zapisovatele serializuje `sync.Mutex` na entry
>   (stejně jako v lazy-cache) — čtenáři ho nikdy neberou.
> - `Get` vrací jen `*T`, `Ready()` není potřeba (`New` blokuje), `Sync` a
>   `Reload` jsou oddělené.

Vysoce výkonná in-memory replika (eventually consistent) pro Go, konzistentní s konvencemi
`moderntv/lazy-cache` a `moderntv/codebook-cache`.

| | |
|---|---|
| Modul | `github.com/moderntv/eventual-cache` |
| Balíček | `eventual` |
| Go | `1.21.0` / `toolchain go1.22.0` (stejně jako sourozenci — nic v návrhu nevyžaduje víc) |
| Typ | `eventual.Cache[T any]`, klíč pevně `int64` |
| Role | **jen replika**, data sama nemění |

**Potvrzená zadání**

- Plná replika celého datasetu, bez evikce, bez měření paměti.
- `New()` **blokuje**, dokud neproběhne první kompletní načtení.
- `Get(id) *T` — jen hodnota, `nil` když položka neexistuje (stejně jako sourozenci).
- Rekonciliace: periodicky se stáhne **seznam všech ID**, chybějící se domažou a nové donačtou.
- Invalidace: **NATS řeší volající**, ne knihovna — ta jen vystaví `Invalidate(id)`
  (přesně jako `lazy-cache`).
- Počet shardů **konfigurovatelný**, mocnina dvou, default 256.
- Cílový HW: servery ~64 jader, jednotky instancí.

---

## 1. Kam eventual-cache patří mezi stávající knihovny

| | lazy-cache | codebook-cache | **eventual-cache** |
|---|---|---|---|
| Co drží | jen to, na co se někdo ptal | celý dataset | celý dataset |
| Plnění | lazy, per položka | full reload celé mapy | blokující warm-up + per-položka refresh |
| Struktura | 1 mapa + 1 `RWMutex` | `atomic.Value` se **celou** mapou | N shardů, každý s `RWMutex` |
| Zápis | pod globálním zámkem | swap celé mapy | per shard, zámek jen při vzniku/zániku |
| `Get` může blokovat na I/O | **ano** (lazy load) | ne | **ne, nikdy** |
| Invalidace | `Invalidate(ID)`, NATS řeší volající | NATS uvnitř, vždy „přenačti vše“ | `Invalidate(ID)`, NATS řeší volající |
| Evikce | TTL watcher (deathrow) | – | jen rekonciliace proti zdroji |

Jednou větou: **codebook-cache s granularitou lazy-cache a bez blokujícího čtení.**

---

## 2. Review předloženého návrhu

Návrh je koncepčně správný — sharding, stale-while-revalidate, deduplikace, mark-and-sweep.
Níže je to, co je potřeba opravit, než z toho bude knihovna.

### P0 — funkční chyby

1. **`c.fetchGroup.Do(string(id), ...)` je špatně.** `string(uint64)` v Go není `"42"`, ale
   znak s daným code pointem. Ověřeno: `string(uint64(42))` == `"*"`, a **jakékoli ID nad
   0x10FFFF se převede na `"�"`** — tedy skoro všechna reálná ID by sdílela jeden
   singleflight klíč a deduplikovala by se navzájem. `go vet` to hlásí:
   `conversion from uint64 to string yields a string of one rune, not a string of digits`.

2. **Singleflight v této architektuře nededupluje vůbec nic — ani s opraveným klíčem.**
   `Do` spouští funkci synchronně a klíč ze své mapy maže *před* návratem. Protože ho volá
   **jediný sekvenční worker** v `for id := range c.fetchCh`, nikdy nejsou dvě volání
   souběžná. Ověřeno: 1000 duplicit stejného ID protažených jedním workerem → **1000
   skutečných fetchů, 0 % deduplikace** (týž `Group` se 100 opravdu souběžnými volajícími
   → 1 fetch). Komentář v návrhu „100 požadavků na ID 42 → spustí se jen 1×“ tedy neplatí.
   Deduplikovat je nutné **před** zařazením do fronty, ne za ní.

3. **`fetchFromExternalAPI` nemá návratovou chybu — přechodný výpadek zdroje maže data.**
   Signatura `(newItem, exists)` neumí odlišit „položka byla smazána“ od „dotaz selhal“.
   Když MariaDB na chvíli neodpovídá, worker projde větví `else` a `delete(shard.data, id)`
   vyhodí platnou položku z repliky. U plné repliky, která je zdrojem pravdy pro čtení,
   je to nejnebezpečnější chyba v celém návrhu.

4. **`IsRefreshing` se nikdy spolehlivě neuvolní.** `newItem.IsRefreshing.Store(false)` se
   volá jen na *nově načtené* položce a jen ve větvi `exists`. Při panice v loaderu
   (nebo po opravě bodu 3, kdy chyba nesmí položku smazat) zůstane stará položka s
   příznakem `true` a už se nikdy sama neobnoví. Nutné `defer`.

5. **TOCTOU v mark-and-sweep.** Mezi `RLock` (mark) a `Lock` (sweep) může worker vložit nebo
   obnovit položku, kterou snapshot `availableIDs` (pořízený před minutou) neobsahoval →
   smažou se platná data. Totéž obráceně: bulk load ve fázi 4 může přepsat novější hodnotu,
   kterou mezitím zapsala invalidace.

6. **Chybí ochrana proti přepsání novější hodnoty starší.** Souběžný per-ID refresh a bulk
   load se můžou navzájem přebít v libovolném pořadí. Tahle třída chyb je pro
   `go test -race` **neviditelná** — všechny operace jsou samostatně atomické, jen jejich
   pořadí je špatné. Musí se testovat deterministicky.

7. **`Get` na neexistující ID spustí fetch pokaždé.** U plné repliky je miss zpravidla
   „položka neexistuje“. Bez tombstone + rate limitu stačí cyklus s náhodnými ID a MariaDB
   dostane plnou zátěž. Nutná negativní cache.

8. **Žádné `context` / shutdown.** `ticker` se nikdy nezastaví, obě goroutiny se nedají
   ukončit, `fetchCh` se nezavírá. Oba sourozenci berou `Context` v `Params` — držme to.

9. **Jeden worker + synchronní `singleflight.Do`** znamená head-of-line blocking: jeden
   pomalý dotaz zastaví obnovu všech ostatních položek.

### P1 — výkon a správnost pod zátěží

10. **`_ [64]byte` padding nefunguje tak, jak má.** Změřeno: `sizeof(Shard)` vyjde **96 B**,
    což není násobek cache line; navíc `shards [256]*Shard` jsou samostatné alokace, takže
    sousední shardy stejně můžou skončit na jedné lince. Řešení: `[]shard[T]` (hodnoty
    v souvislé alokaci, ne pointery) a padding na přesně 128 B, hlídaný při překladu.

11. **„Zero-Allocation“ neplatí.** `idsByShard := make([][]uint64, 256)` plus `append` do
    vnitřních slices alokuje při každém ticku. Buffery musí být na struktuře cache
    a recyklovat se přes `buf[:0]`.

12. **256 goroutin na tick + `sync.Mutex` kolem `missingIDs`** je zbytečné. Stačí worker pool
    velikosti `GOMAXPROCS` a per-shard výstupní slice bez zámku.

13. **Bulk load zapisuje položku po položce s `Lock`/`Unlock` na každou.** Seskupit podle
    shardu → jeden zámek na shard.

14. **`fetchCh` se plní duplicitami.** 1000 missů na stejné ID zabere 1000 slotů fronty.

15. **`time.Now().Unix()` v hot pathu.** Změřeno v testovacím prostředí: **64 ns/op** oproti
    0,7 ns na hash. I na bare metal (~25 ns) je to řádově víc než zbytek `Get`. Sekundová
    granularita je navíc hrubá — sourozenci používají `UnixMilli`. Návrh níže tuhle kontrolu
    z read pathu **odstraňuje úplně**.

16. **Chybí jitter na sync intervalu.** Všechny instance služby se restartují společně
    a pak trefí zdroj ve stejnou sekundu. Oba sourozenci mají `Randomizer` — použít.

17. **Rekonciliace porovnává jen přítomnost ID, ne obsah.** Podle zadání je to takhle
    záměrně — o obsah se stará zdroj tím, že při každé změně pošle invalidaci.

18. **Chybí metriky a logování.** Obě sourozenecké knihovny mají cadre metriky + zerolog.

### P2 — API a drobnosti

19. `Get` vrací `*Item`, tedy interní strukturu s `ExpiresAt` a `IsRefreshing`. Má vracet
    payload (`*T`), jako to dělají oba sourozenci.
20. `atomic.Bool` uvnitř `Item` obsahuje `noCopy` → jakékoli kopírování `Item` hodnotou
    spustí `go vet: copylocks`. Metadata patří do obalového `entry[T]`, ne do payloadu.
21. `triggerFetch` tiše zahazuje požadavky. Musí být vidět v metrice a v logu.
22. Chybí `Len()`, `ForEach()`, `Sync()`, `Stats()`, `Close()`.

---

## 3. Hashovací funkce pro výběr shardu

### Proč `id & (N-1)` u vás selhává

Galera nastavuje `auto_increment_increment` na počet nodů, takže ID tvoří aritmetickou
posloupnost s krokem *K*. Pro `id % 256` rozhoduje `gcd(K, 256)`: obsadí se jen
`256 / gcd(K,256)` shardů.

Změřeno na 100 000 ID, 256 shardů (`max/avg` = zatížení nejplnějšího shardu vůči průměru):

| Vzor ID | `id & 255` prázdných | `id & 255` max/avg | `fib >>56` prázdných | `fib >>56` max/avg |
|---|---|---|---|---|
| krok 1 | 0 | 1,00 | 0 | 1,01 |
| krok 2 | **128** | **2,00** | 0 | 1,01 |
| krok 3 | 0 | 1,00 | 0 | 1,01 |
| krok 4 | **192** | **4,00** | 0 | 1,01 |
| **krok 6 (váš případ)** | **128** | **2,00** | 0 | **1,00** |
| krok 8 | **224** | **8,00** | 0 | 1,01 |
| krok 10 | **128** | **2,00** | 0 | 1,01 |
| krok 16 | **240** | **16,00** | 0 | 1,01 |
| Galera 6 nodů, všech 6 aktivních | 0 | 1,00 | 0 | 1,01 |
| Galera 6 nodů, jen 3 aktivní | 0 | 1,34 | 0 | 1,01 |
| náhodná ID | 0 | 1,18 | 0 | 1,14 |

Při kroku 6 leží **polovina shardů prázdná** a druhá polovina má dvojnásobek položek
i dvojnásobek zámkové kontence. A protože počet nodů clusteru se může změnit, jednou to může
být 8 (osminásobek) nebo 4 (čtyřnásobek). Maskování spodních bitů je pro tenhle tvar ID
nepoužitelné.

### Rychlost (Intel Xeon 2,1 GHz, 1 jádro)

| Funkce | ns/op | alokace |
|---|---|---|
| `id & 255` | 0,690 | 0 |
| **`(id * φ64) >> 56`** | **0,702** | **0** |
| xxhash-style mix (2 mul, 2 xor-shift) | 0,985 | 0 |
| splitmix64 finalizer | 1,307 | 0 |
| `hash/maphash.Comparable` (Go 1.24) | 4,907 | 0 |

### Doporučení

```go
const fibonacci64 = 0x9E3779B97F4A7C15 // 2^64 / φ, liché

// bits = log2(počet shardů)
func MultiplyShiftHash(id int64, bits uint) uint32 {
    return uint32((uint64(id) * fibonacci64) >> (64 - bits))
}
```

Je **stejně rychlý jako maskování** (0,70 vs 0,69 ns, rozdíl je v šumu) a na aritmetických
posloupnostech dokonce *lepší* než silné hashe — vychází z rovnoměrného rozložení zlatého
řezu, takže postupné hodnoty se rozprostírají skoro dokonale (`max/avg` 1,00–1,01 oproti
1,12–1,20 u splitmixu). Podmínkou je brát **horní** bity součinu, ne spodní.

Ověřeno testem v referenční kostře: pro počty shardů 256 / 512 / 1024 a kroky ID
1, 2, 3, 4, 6, 8, 10, 16 je vždy 0 prázdných shardů a `max/avg < 1,02`.

Detaily:

- Počet shardů musí být mocnina dvou (aby `bits` fungovalo posunem) — `Params.check()`
  to ověří.
- Funkci vystavit v `Params` jako `ShardHash ShardHashFunc`, aby šla přebít bez forku.
  Knihovna nabídne `MultiplyShiftHash` (default) a `SplitMix64Hash`.
- Multiply-shift **není** kryptograficky odolný — kdo zná konstantu, umí si vyrobit ID
  mířící do jednoho shardu. Pro interní DB ID to nevadí; kdyby cache někdy klíčovala podle
  hodnot z venku, přepnout na `SplitMix64Hash` (stále jen 1,3 ns).

### Kolik shardů

Shard stojí **128 B** (ověřeno, nezávisle na `T`), takže i 4096 shardů je 512 kB — počet
shardů se volí podle souběžnosti, ne podle paměti.

| Situace | Doporučení |
|---|---|
| default | **256** |
| stroje ~64 jader, vysoký read RPS | **1024** (= 16× `GOMAXPROCS`) |
| malý dataset (< 10 tis. položek) | 256 stačí; víc shardů jen zdraží iteraci při rekonciliaci |
| velmi velký dataset (> 5 mil. položek) | 2048–4096, hlavně kvůli délce `Lock` při dávkovém mazání |

Pravidlo palce: **8–16× `GOMAXPROCS`**, zaokrouhleno nahoru na mocninu dvou. Protože je to
parametr, dá se doladit benchmarkem na cílovém stroji, ne odhadem.

---

## 4. Architektura

### 4.1 Konzistenční model

Cache je asynchronní read-only replika. O aktuálnost se starají tři nezávislé mechanismy:

1. **Blokující warm-up** v `New()` — `LoadAllFunc` natáhne kompletní dataset. Když selže,
   `New()` vrátí chybu a služba nenaběhne s prázdnou replikou.
2. **Invalidace od volajícího** — `Invalidate(id)` / `InvalidateMultiple(ids)` /
   `InvalidateAll()`. Volající je zavolá, když mu z NATS přijde zpráva. Latence v jednotkách
   ms. Knihovna o NATS neví (viz 4.8).
3. **Periodická rekonciliace** — každých `SyncInterval` (default 5 min ± jitter) se stáhne
   seznam všech ID, chybějící se domažou a nové donačtou.

Protože NATS není 100% spolehlivý a rekonciliace porovnává jen **přítomnost** ID, existuje
scénář „zpráva se ztratila, položka existuje dál, ale její obsah je zastaralý navždy“.


### 4.2 Čtecí cesta

Klíčové rozhodnutí: **v `Get` není žádná časová logika.** Veškerá čerstvost se řeší na
pozadí, takže read path je jen hash + `RLock` + lookup + atomický load. Odpadá tím
`time.Now()` (64 ns) i CAS na `refreshing`.

```go
// Get vrací hodnotu z lokální repliky, nebo nil když položka neexistuje.
// Nikdy neblokuje na I/O.
func (c *Cache[T]) Get(id int64) *T {
    sh := c.shardOf(id)

    sh.mu.RLock()
    e, exists := sh.data[id]
    sh.mu.RUnlock()

    if !exists {
        c.onMiss(id) // deduplikované a rate-limitované

        return nil
    }

    return e.state.Load().value // nil == tombstone
}
```

Naměřeno na 2 jádrech, 200 000 položek, 256 shardů:

| | sériově | paralelně |
|---|---|---|
| shardovaný `RWMutex` | 36,3 ns | 24,5 ns |
| **všechny goroutiny do jednoho shardu** (špatný hash) | – | **71,7 ns** |

Už na dvou jádrech je špatný hash 3× dražší. Na 64 jádrech je to řádový rozdíl — to je druhý
argument pro sekci 3.

### 4.3 Proč shardovaný `RWMutex` a ne copy-on-write

Zvažoval jsem i variantu „per shard `atomic.Pointer[map]`, zápis přestaví mapu shardu“ —
čtení je pak úplně bez zámku. Změřeno:

| | čtení sériově | čtení paralelně | **jeden zápis** | alokace na zápis |
|---|---|---|---|---|
| shardovaný `RWMutex` | 36,3 ns | 24,5 ns | **124 ns** | 16 B |
| copy-on-write per shard | 27,7 ns | 13,8 ns | **31 429 ns** | **18 544 B** |

COW čte ~1,8× rychleji, ale jeden zápis stojí o dva řády víc a vyrobí 18 kB odpadu — přesně
to, čemu se chceme vyhnout kvůli GC. Pro plnou repliku s průběžnými invalidacemi je
`RWMutex` správná volba.

Navíc: v navrženém modelu **obnova hodnoty vůbec nebere zápisový zámek.** Pointer na
`entry[T]` v mapě je stabilní; mění se jen atomický stav uvnitř. Zápisový zámek je potřeba
jen při vzniku a zániku položky, tedy u plné repliky výjimečně.

### 4.4 Datové struktury

Hodnota, verze a razítko načtení musí být **jeden nedělitelný celek**. Kdyby to byly
samostatné atomiky, vznikne ztracená aktualizace: zapisovatel A přečte starý stav, uspí se,
zapisovatel B mezitím doběhne celý, a pak A dopíše svou (starší) hodnotu přes novější.
`go test -race` na to neupozorní, protože každá operace zvlášť atomická je. Proto jeden
`atomic.Pointer` a CAS smyčka:

```go
// state je neměnný snapshot položky. Nikdy se nemutuje, jen nahrazuje přes CAS.
type state[T any] struct {
    value    *T    // nil == tombstone (ve zdroji neexistuje)
    version  int64 // volitelné; 0 == verzování se nepoužívá
    loadedAt int64 // ms, kdy byl tento stav načten ze zdroje (ochrana sweepu)
    // expiresAt v ms: kdy znovu ověřit tombstone (NotFoundTTL). 0 u živé hodnoty.
    expiresAt int64
}

type entry[T any] struct {
    state      atomic.Pointer[state[T]]
    refreshing atomic.Bool // dedup: položka už je ve frontě na obnovu
}

type shardCore[T any] struct {
    mu   sync.RWMutex
    data map[int64]*entry[T]
}

type shard[T any] struct {
    shardCore[T]
    _ [cacheLinePadBytes - unsafe.Sizeof(shardCore[struct{}]{})]byte
}

// kontrola při překladu, že shard je přesně násobek cache line
var _ = [1]struct{}{}[unsafe.Sizeof(shard[struct{}]{})%cacheLinePadBytes]
```

`deadline` schválně slučuje dvě věci, které se nikdy nepotkají: živá položka se obnovuje,
tombstone expiruje. Ušetří to jedno pole a drží `state` na 32 B.

Zápis:

```go
// apply zapíše nový stav, pokud není starší než ten současný.
func (e *entry[T]) apply(next *state[T]) (applied bool) {
    for {
        cur := e.state.Load()
        if cur != nil && next.version != 0 && cur.version > next.version {
            return false // dorazila starší verze, ignorujeme
        }
        if e.state.CompareAndSwap(cur, next) {
            return true
        }
        // někdo nás předběhl, načteme znovu a rozhodneme podle nové verze
    }
}
```

`Version` v `LoadedEntry` je volitelný (0 = nepoužívá se). Když ho zdroj nedodá, platí
last-write-wins — CAS smyčka pak zajišťuje jen atomicitu dvojice (hodnota, razítko), ne
uspořádání. Kdyby se někdy `updated_at` doplnil, stačí ho vyplnit a ordering začne platit
bez dalších změn.

Ověřeno na skutečném kódu (`gofmt`, `go vet`, `go test -race` čistě, Go 1.21):

| | velikost |
|---|---|
| `shard[T]` | **128 B**, nezávisle na `T` (ověřeno pro `struct{}`, malou strukturu i `[512]byte`) |
| `entry[T]` | 16 B |
| `state[T]` | 32 B |
| 1024 shardů | 128 kB |

Test `TestApplyConcurrentHighestWins` pouští 200 kol po 32 souběžných zapisovatelích
s různými verzemi a pokaždé musí zvítězit nejvyšší — s CAS smyčkou prochází, se dvěma
oddělenými atomiky ne.

Shardy jako `[]shard[T]` (hodnoty v souvislé alokaci), **ne** `[]*shard[T]` — jinak padding
nic neřeší.

### 4.5 Zápisové cesty

| Událost | Zámek | Poznámka |
|---|---|---|
| obnova hodnoty existující položky | jen `RLock` na dohledání entry | zápis přes `entry.apply()` (CAS) |
| vznik nové položky | `Lock` na shard | + kontrola závodu (mezitím ji mohl vložit někdo jiný) |
| zánik položky (rekonciliace) | `Lock` na shard, dávkově | jeden zámek na shard, ne na položku |
| tombstone | jako obnova | `value = nil`, `deadline` = `NotFoundTTL` |

Pravidla platná pro **všechny** zápisové cesty bez výjimky (invalidace, dávkové načtení,
rekonciliace, warm-up):

- zápis jde vždy přes `entry.apply()` s CAS smyčkou — nikdy přímým `Store`,
- chyba jiná než `ErrNotFound` starou hodnotu **nepřepisuje** a nikdy nevede ke smazání
  položky (stejná politika jako `lazy-cache.addLoadedEntry`),
- **rekonciliace používá stejnou dedup bránu (`refreshing` / `pending`) jako `Invalidate`.**
  Kdyby měla vlastní cestu, mohly by dva workery zapisovat do téže entry souběžně.
  Jedna brána = jedno pořadí.

### 4.6 Refresh worker pool

- Fronta `refreshCh chan int64`, deduplikace **před** zařazením:
  - položka v mapě → `e.refreshing.CompareAndSwap(false, true)`
  - položka mimo mapu → `pending sync.Map` (`LoadOrStore`)
- `RefreshWorkers` goroutin (default 4), ne jedna.
- **Dávkování:** worker sbírá ID z kanálu do dávky (`RefreshBatchSize`, default 200, nebo
  `RefreshBatchDelay`, default 20 ms) a volá `LoadMultipleFunc` — jeden
  `SELECT … WHERE id IN (…)` místo 200 dotazů. `LoadOneFunc` je fallback, když
  `LoadMultipleFunc` není nastavená.
- Při plné frontě se požadavek zahodí, `refresh_dropped_total`++ a warn log (rate-limited).
- `defer` vždy uklidí `refreshing` / `pending`, i při chybě, i při panice.
- Chyba načtení → hodnota zůstává, další pokus nejdřív za `ErrorRetryInterval`.
- Miss na neznámé ID prochází token bucketem (`MissRateLimit`, default 100/s), aby náhodná
  ID zvenčí nemohla zahltit zdroj. Výsledek „neexistuje“ se uloží jako tombstone
  na `NotFoundTTL`.

### 4.7 Periodická rekonciliace

Běh každých `SyncInterval` (± `Randomizer`):

1. `syncStartedAt := now()`; `ids, err := ListIDsFunc(ctx)` → `[]int64`. Při chybě se běh
   **zruší celý** (nic se nemaže) a jen se zvýší chybová metrika.
2. Naplnit recyklovanou množinu `available map[int64]struct{}` (`clear()` + vložení, žádná
   nová alokace).
3. **Sweep** — projít shardy paralelně (pool `GOMAXPROCS`, ne goroutina na shard):
   - položka **není** v `available` **a** `state.loadedAt < syncStartedAt` → smazat
   - tombstone s `deadline < now` → smazat
   - mazání dávkově: nasbírat ID pod `RLock`, pak jeden `Lock` na shard; těsně před
     `delete` znovu ověřit `loadedAt < syncStartedAt` (mezitím mohl dorazit refresh)
4. **Fill** — ID z `available`, která v replice chybí → dávkově přes `LoadMultipleFunc`,
   seskupeně po shardech.
5. Buffery (`available`, `toDelete`, `toRefresh`, `missing`, per-shard slices) jsou pole
   na struktuře cache a recyklují se přes `buf[:0]` — tohle je to skutečné
   „zero-allocation“, ověřitelné přes `-benchmem`.
6. Metriky: doba běhu, added / removed / refreshed, timestamp posledního úspěchu.
7. Volitelný hook `OnSync(SyncStats)` (obdoba `OnReload` v codebook-cache).

**Podmínka `loadedAt < syncStartedAt` je to, co řeší TOCTOU z bodu P0-5.** Pokrývá obě
varianty: položku, která během běhu syncu vznikla, i položku, která se během něj obnovila
(typicky invalidace doražená v okně mezi Mark a Sweep). Obě mají `loadedAt` novější než
začátek syncu, takže je sweep nesmaže.

**Chyba `ListIDsFunc` neznamená mazání.** Když se seznam nepodaří stáhnout, běh se
zruší celý a replika zůstane přesně taková, jaká byla.

### 4.8 Invalidace — řeší volající

Knihovna o NATS neví, stejně jako `lazy-cache`. Vystaví jen:

```go
func (c *Cache[T]) Invalidate(id int64)
func (c *Cache[T]) InvalidateMultiple(ids []int64)
func (c *Cache[T]) InvalidateAll() // označí vše k obnově; fronta se plní postupně
```

Ve službě to pak vypadá takhle:

```go
natsConn.Subscribe("cache.channel.updated", func(msg *nats.Msg) {
    var event pb.ChannelUpdated
    if err := proto.Unmarshal(msg.Data, &event); err != nil {
        log.Warn().Err(err).Msg("invalid invalidation message")
        return
    }

    cache.InvalidateMultiple(event.GetIds())
})
```

Důsledek pro `go.mod`: **žádná závislost na `nats.go` ani na `protobuf`.** Zůstává jen
`cadre` (metriky), `zerolog` a `testify` pro testy.

`InvalidateAll()` neprovádí okamžitý full reload — jen označí všechny položky k obnově,
takže se dávkově protečou frontou. Kdo chce tvrdý reload, zavolá `Sync(ctx)`.

---

## 5. Veřejné API

Podpisy níže jsou bez těl. Referenční kostra s těly (datové struktury z 4.4, `Get`,
`GetMultiple`, `Invalidate`, `apply`, `applyLoaded`, `ForEach`) je přiložená jako
`_design/skeleton/` — překládá se pod Go 1.21, prochází `gofmt`, `go vet` i `go test -race`.

```go
package eventual

// --- konstruktor (blokuje do prvního kompletního načtení) ---
func New[T any](params Params[T]) (c *Cache[T], err error)

// --- čtení (nikdy neblokuje na I/O) ---
func (c *Cache[T]) Get(id int64) *T
func (c *Cache[T]) GetMultiple(ids []int64) (values map[int64]*T)
func (c *Cache[T]) ForEach(fn func(id int64, value *T) bool)
func (c *Cache[T]) Len() int

// --- invalidace (volá je uživatel knihovny, např. z NATS handleru) ---
func (c *Cache[T]) Invalidate(id int64)
func (c *Cache[T]) InvalidateMultiple(ids []int64)
func (c *Cache[T]) InvalidateAll()

// --- životní cyklus ---
func (c *Cache[T]) Sync(ctx context.Context) error // vynucená rekonciliace
func (c *Cache[T]) Stats() Stats
func (c *Cache[T]) Close() // zastaví goroutiny a počká na ně
```

```go
// ShardHashFunc mapuje ID na index shardu v rozsahu [0, 1<<bits).
type ShardHashFunc func(id int64, bits uint) uint32

func MultiplyShiftHash(id int64, bits uint) uint32 // default
func SplitMix64Hash(id int64, bits uint) uint32

type LoadedEntry[T any] struct {
    ID      int64
    Value   *T    // nil == neexistuje
    Version int64 // volitelné; 0 == verzování se nepoužívá
    Err     error
}

type (
    LoadOneFunc[T any]      func(ctx context.Context, id int64) (value *T, version int64, err error)
    LoadMultipleFunc[T any] func(ctx context.Context, ids []int64) (entries []LoadedEntry[T], err error)
    LoadAllFunc[T any]      func(ctx context.Context) (entries []LoadedEntry[T], err error)
    ListIDsFunc             func(ctx context.Context) (ids []int64, err error)
)

type Params[T any] struct {
    Context         context.Context
    Log             zerolog.Logger
    MetricsRegistry *cadre_metrics.Registry
    Name            string

    // LoadAllFunc načte kompletní dataset. Volá se z New() (blokující warm-up).
    LoadAllFunc LoadAllFunc[T] // povinné
    // ListIDsFunc vrací seznam všech platných ID. Volá ji periodická rekonciliace.
    ListIDsFunc ListIDsFunc // povinné
    // LoadOneFunc načte jednu položku. Fallback, když není LoadMultipleFunc.
    LoadOneFunc LoadOneFunc[T] // povinné
    // LoadMultipleFunc načte dávku položek jedním dotazem.
    LoadMultipleFunc LoadMultipleFunc[T] // volitelné, silně doporučené

    Timeouts Timeouts

    Shards            int           // mocnina 2, default 256 (viz sekce 3)
    ShardHash         ShardHashFunc // default MultiplyShiftHash
    RefreshWorkers    int           // default 4
    RefreshQueueSize  int           // default 10000
    RefreshBatchSize  int           // default 200
    RefreshBatchDelay time.Duration // default 20ms
    MissRateLimit     int           // dotazů/s na neznámá ID, default 100

    OnSync func(stats SyncStats) // volitelný hook po každé rekonciliaci
}

type Timeouts struct {
    SyncInterval       time.Duration `mapstructure:"sync_interval"`        // default 5m
    NotFoundTTL        time.Duration `mapstructure:"not_found_ttl"`        // default 1m
    ErrorRetryInterval time.Duration `mapstructure:"error_retry_interval"` // default 10s
    Randomizer         float64       `mapstructure:"randomizer"`           // default 0.1
}

type Stats struct {
    Items         int
    Tombstones    int
    QueueLength   int
    LastSyncAt    time.Time
    LastSyncStats SyncStats
}

type SyncStats struct {
    Added     int
    Removed   int
    Refreshed int
    Duration  time.Duration
    Err       error
}
```

`Params.check()` a `Timeouts.check()` stejně jako u sourozenců (vracejí `error`, volané
z `New`). `check()` navíc ověří, že `Shards` je mocnina dvou.

---

## 6. Struktura repozitáře

```
eventual-cache/
├── cache.go           Cache[T], New, Get, GetMultiple, ForEach, Len, Invalidate*, Sync, Stats, Close
├── entry.go           state[T], entry[T], apply()
├── shard.go           shard[T], shardOf, paralelní iterace přes shardy
├── hash.go            ShardHashFunc, MultiplyShiftHash, SplitMix64Hash
├── params.go          Params[T], loadery, LoadedEntry, check()
├── timeouts.go        Timeouts + check()
├── refresh.go         fronta, dedup, worker pool, dávkování, rate limit, tombstones
├── sync.go            periodická rekonciliace (sweep + fill)
├── stats.go           Stats, SyncStats
├── error.go           ErrNotFound
├── internal/
│   ├── metrics/       Prometheus přes cadre (podsystém eventual_cache)
│   ├── utils/         RandomizeDuration (převzato z lazy-cache)
│   └── test_utils/    logger, metrics, pointer helpers
├── hash_test.go       distribuce + rychlost
├── cache_test.go      TestCache s podtesty (styl lazy-cache)
├── sync_test.go
├── refresh_test.go
├── bench_test.go
├── Makefile           test, test-coverage
├── README.md
├── go.mod
└── .gitignore
```

Bez `.github` (stejně jako lazy-cache), bez `internal/memsize`, bez `internal/invalidation`.

`go.mod`:

```
module github.com/moderntv/eventual-cache

go 1.21.0

toolchain go1.22.0

require (
    github.com/moderntv/cadre v0.4.6
    github.com/prometheus/client_golang v1.14.1-0.20221122130035-8b6e68085b10
    github.com/rs/zerolog v1.20.0
    github.com/stretchr/testify v1.7.1
)
```

Konvence převzaté ze sourozenců: pojmenované návratové hodnoty s `naked return`, `check()`
na `Params`/`Timeouts`, `metrics_pkg` alias importu, zerolog s `Str("cache", name)`,
`mapstructure` tagy na `Timeouts`, testify `assert`, jeden `TestCache` s `t.Run` podtesty,
Makefile jen s `test` a `test-coverage`.

---

## 7. Fáze implementace

| Fáze | Obsah | Hotovo, když |
|---|---|---|
| **0. Skeleton** | repo, `go.mod`, `Makefile`, `.gitignore`, kostra README | `make test` projde naprázdno |
| **1. Úložiště + hash** | `shard.go`, `hash.go`, `entry.go`, `Get`, `GetMultiple`, `ForEach`, `Len` | distribuční test prochází pro 256/512/1024 shardů a kroky ID 1–16; benchmark `Get` sériově i paralelně; `TestApplyConcurrentHighestWins` prochází |
| **2. Warm-up** | `params.go`, `check()`, `LoadAllFunc`, blokující načtení v `New()` | `New()` vrátí chybu, když warm-up selže; po návratu je replika kompletní |
| **3. Refresh** | `refresh.go` — fronta, dedup, worker pool, dávkování, tombstones, rate limit, backoff | 1000 **souběžných** missů na jedno ID → 1 volání loaderu; 1000 missů **rozprostřených v čase** přes malý pool → taky max. pár volání (přesně tohle v originále tiše selhalo); opakovaný miss na neexistující ID → 1 volání za `NotFoundTTL` |
| **4. Rekonciliace** | `sync.go` — sweep, fill, recyklované buffery | TOCTOU testy: (a) položka **vložená** během syncu přežije, (b) položka **obnovená** během syncu přežije; chyba `ListIDsFunc` nic nesmaže; `-benchmem` ukáže 0 alokací na tick po zahřátí |
| **5. Invalidace** | `Invalidate`, `InvalidateMultiple`, `InvalidateAll`, `Sync` | invalidace projde stejnou dedup bránou jako sync; `InvalidateAll` nezahltí frontu |
| **6. Observabilita** | metriky, `Stats()`, logování | metriky se registrují, `Stats()` sedí s realitou |
| **7. Zpevnění** | `-race` zátěžové testy, benchmarky na 64jádrovém stroji (ladění `Shards`), README, tag `v0.1.0` | `go test -race ./...` čistě; README na úrovni lazy-cache |
| **8. Pilot** | nasadit do jedné služby vedle stávajícího řešení, porovnat metriky | p99 čtení, RPS na MariaDB, RSS |

Fáze 1–4 jsou sériově závislé, fáze 5 a 6 se dají dělat paralelně po fázi 3.

---

## 8. Testovací strategie

- **Souběžnost:** stejný vzor jako `testCacheParallelism` v lazy-cache — 100 goroutin ×
  100 000 iterací, mix `Get` / `Invalidate` / souběžný `Sync`, vše pod `-race`.
- **Determinismus:** fake loader s atomickými čítači volání; testy tvrdí *kolik* volání
  proběhlo, ne jen výsledek.
- **TOCTOU:** `ListIDsFunc` schválně zdrží odpověď; mezitím (a) vznikne nová položka a
  (b) se obnoví existující položka, kterou snapshot neobsahuje → **ani jedna nesmí zmizet**.
- **Verzování — deterministicky, ne přes `-race`:** `go test -race` třídu „ztracená
  aktualizace“ nezachytí, protože jednotlivé operace atomické jsou a chybné je jen jejich
  pořadí. Testovat s explicitní synchronizací: zapisovatel s verzí 101 se pozastaví uprostřed
  `apply()`, doběhne zapisovatel s verzí 102, pak se 101 pustí dál → výsledek musí být 102.
  Plus stochastická varianta: 200 kol × 32 souběžných zapisovatelů, vždy vyhraje nejvyšší.
- **Odolnost zdroje:** loader vrací chyby → stará data zůstávají, položky se nemažou,
  `refreshing` se uvolní, po `ErrorRetryInterval` je další pokus. `ListIDsFunc` vrátí chybu
  → **replika zůstane nedotčená**, běh syncu se zruší celý.
- **Tombstone:** `NotFoundTTL` platí, po vypršení jeden nový pokus.
- **Shutdown:** `Close()` po sobě uklidí; `goleak` na konci testů.
- **Distribuce hashe:** pro 256 / 512 / 1024 shardů a kroky ID 1–16 nula prázdných shardů
  a `max/avg < 1,5`; navíc na reálném vzorku ID, když bude k dispozici.
- **Benchmarky:** `Get` sériově i paralelně (na 64jádrovém stroji pro volbu `Shards`),
  `Invalidate`, jeden běh rekonciliace na 1 mil. položkách, vše s `-benchmem`.

---

## 9. Metriky (podsystém `eventual_cache`, label `name`)

| Metrika | Typ | Význam |
|---|---|---|
| `items_count` | Gauge | položky v replice (bez tombstones) |
| `tombstones_count` | Gauge | negativní záznamy |
| `reads_total` | Counter | volání `Get` / `GetMultiple` |
| `hits_total` / `misses_total` | Counter | trefy a minutí |
| `refresh_enqueued_total` | Counter | zařazeno do fronty |
| `refresh_dropped_total` | Counter | **zahozeno kvůli plné frontě** — alertovat |
| `refresh_batches_total` | Counter | volání `LoadMultipleFunc` |
| `load_errors_total` | Counter | chyby loaderu mimo `ErrNotFound` |
| `invalidations_total` | Counter | volání `Invalidate*` |
| `sync_runs_total` | Counter | běhy rekonciliace |
| `sync_errors_total` | Counter | běhy, které skončily chybou nebo se přeskočily |
| `sync_duration_seconds` | Histogram¹ | doba běhu rekonciliace |
| `sync_added` / `sync_removed` / `sync_refreshed` | Counter | co rekonciliace dorovnala |
| `last_sync_timestamp` | Gauge | **stáří poslední rekonciliace** — alertovat |
| `queue_length` | Gauge | délka fronty |

¹ Ověřeno v `cadre/metrics/collectors.go`: registry nabízí jen `NewCounter`, `NewCounterVec`,
`NewGauge`, `NewGaugeVec` a `NewSummaryVec` — helper na histogram tam **není**. Metoda
`Register(name string, c prometheus.Collector)` ale bere libovolný collector, takže stačí
`prometheus.NewHistogram(...)` a zaregistrovat ručně (případně použít `SummaryVec`).

Alerty, které dávají smysl hned:

- `refresh_dropped_total` roste → fronta nestíhá, zvýšit `RefreshQueueSize` / `RefreshWorkers`,
- `time() - last_sync_timestamp > 3 × SyncInterval` → rekonciliace stojí,
- `sync_errors_total` roste → zdroj nedostupný, replika stárne,
- skokové `sync_removed` → stojí za pohled, zdroj mohl vrátit neúplný seznam.

---

## 10. Zbývající otevřené otázky

1. **Kolik položek dataset má a jak velké jsou?** Struktury „několik stovek bajtů“ znamenají,
   že 1 mil. položek ≈ 300–500 MB payloadu + ~50 MB režie cache. Při jednotkách instancí je
   to potřeba potvrdit proti dostupné RAM.
2. **Jak dlouho trvá `LoadAllFunc`?** Blokující warm-up prodlužuje start služby o tuhle dobu.
   Pokud jsou to desítky sekund, stojí za zvážení readiness probe, která to zohlední.
3. **Vzorek reálných ID.** Dej mi `SELECT id FROM …` do textového souboru a proženu ho
   měřicím harnessem; do plánu pak doplním čísla místo syntetických.
4. **Kolik shardů nastavit?** Default 256, pro 64jádrové stroje doporučuju `Shards: 1024`.
   Stojí za to to na cílovém HW proměřit přiloženými benchmarky (`go test -bench Get`).

---

## Přílohy

- `_design/shardhash/` — měřicí harness pro hashovací funkce: čtyři kandidáti, generátory ID
  (aritmetické kroky, Galera s N nody a M aktivními, náhodná ID, mix rozsahů), výpočet
  `max/avg`, prázdných shardů a chí-kvadrátu. Doplň `real_ids.txt` a spusť `go run .`;
  čísla v sekci 3 pocházejí odtud.
- `_design/skeleton/` — referenční kostra datových struktur a čtecí/zápisové cesty
  z kapitoly 4, včetně testů uspořádání verzí, velikosti shardu a distribuce hashe.
  Překládá se pod Go 1.21 a prochází `go test -race`. Je to použitelný první commit fáze 1.
