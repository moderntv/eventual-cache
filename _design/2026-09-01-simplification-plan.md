# eventual-cache — zjednodušení: zadání, rozhodnutí a stav

**Stav: implementováno** (1. 9. 2026). Tento dokument je záznam zadání a
rozhodnutí, ne plán k vykonání — kód i `README.md` už cílový stav popisují.
Původní návrh knihovny je v `_design/IMPLEMENTATION_PLAN.md` a z velké části
neplatí.

## Zadání

1. **Styl `if`** — u `if` se nepoužívá přiřazení a porovnání na jednom řádku,
   přiřazení se vytáhne na samostatný řádek. Platí pro všechny Go projekty.
2. **API** — jen `Get`, `Invalidate`, `Close`.
3. **Verze Go** — `go` i `toolchain` na 1.22.0.
4. **Načítání** — žádné `LoadAllFunc` a `LoadOneFunc`. Po instanciaci se stáhne
   seznam všech ID (`ListIDsFunc`) a přes `LoadMultipleFunc` se načtou po dávkách
   `BatchSize`, konfigurovatelné parametrem.
5. **Invalidace** — `Invalidate` u položky jen nastaví příznak. Další goroutina
   označené položky hromadně načítá (po sekundě, nebo když se dosáhne `BatchSize`).
6. **Položka** — žádné `version` a `seq`. Položka se maže, až když načtení
   dokáže, že ji zdroj nemá (`nil` hodnota nebo `ErrNotFound`).
7. **`LoadedEntry.Version`** — zahodit.
8. **`Get` na neexistující ID** — vrátí `nil`, ale ID se přidá do seznamu
   příště načítaných; pravidelná goroutina se ho pak pokusí načíst.
9. **TTL** — položka má TTL. Po expiraci `Get` **dál vrací hodnotu**, ale chová
   se, jako by se položka invalidovala (přidá se do seznamu k přenačtení).
10. **Metriky** — zůstávají sharded: každý shard inkrementuje svůj čítač a
    goroutina je sbírá, aby se kvůli metrikám neblokovalo čtení.
11. **Mazání při syncu** — chybějící položky se při prvním syncu jen označí
    `to-be-deleted` a smažou se až při dalším syncu, pokud je příznak už nastaven.

## Jak to nakonec je

### Veřejné API

```
func New[T any](params Params[T]) (*Cache[T], error)
func (c *Cache[T]) Get(ID int64) *T
func (c *Cache[T]) Invalidate(ID int64)
func (c *Cache[T]) Close()
```

Plus `MultiplyShiftHash`, `SplitMix64Hash`, `ErrNotFound` a typy `Params`,
`Timeouts`, `LoadedEntry`, `LoadMultipleFunc`, `ListIDsFunc`, `ShardHashFunc`.

Zrušeno: `GetMultiple`, `ForEach`, `Len`, `InvalidateMultiple`, `InvalidateAll`,
`Sync`, `Reload`, `Stats`, `SyncStats`, `Params.OnSync`, `LoadAllFunc`,
`LoadOneFunc`, `LoadedEntry.Version`, `Timeouts.NotFoundTTL`,
`Timeouts.ErrorRetryInterval`, `RefreshWorkers`, `RefreshQueueSize`,
`RefreshBatchDelay`, `ErrReloadInProgress`, celý `stats.go` a tombstony.

### Položka

```go
type entry[T any] struct {
	value             atomic.Pointer[T]
	refreshAt         atomic.Int64
	invalidated       atomic.Bool
	markedForDeletion atomic.Bool
}
```

`refreshAt` je TTL — okamžik, kdy se má položka přenačíst, **ne** kdy vyprší.
`version` ani `seq` položka nemá.

### Jedna goroutina

`run()` v `refresh.go` obsluhuje pět tiků: přenačtení označených položek
(`RefreshInterval`), signál „je jich na dávku", rekonciliaci (`SyncInterval`),
hrubé hodiny (100 ms) a sběr metrik (1 s). Protože rekonciliace i přenačítání
běží ve stejné goroutině, nemohou se proplést — proto není potřeba `syncMu` ani
`seq` na položce.

### Hrubé hodiny

`Get` musí u TTL porovnat čas, a `time.Now()` by na čtecí cestě stálo víc než
celý zbytek `Get`. Goroutina proto každých 100 ms ukládá `time.Now().UnixMilli()`
do `c.nowMillis` a `Get` si ho přečte jedním atomickým loadem. Stejné hodiny
používá i rate limiter misses, takže je bezzámkový.

Naměřeno: `Get` s TTL 6,5 ns/op vs. 6,3 ns/op bez TTL — v šumu.

### Dvoufázové mazání

Tvůj návrh, převzatý beze změny. Je to lepší řešení než `seq`: `seq` chránil
položku jen v rámci jednoho běhu rekonciliace, dvoufázové mazání jí dá celý další
`SyncInterval`. Navíc pokrývá i případ, kdy je zastaralý sám výpis ID ze zdroje
(read z replikace), na což `seq` nestačil.

Cena: smazaná položka zůstane v cache o jeden `SyncInterval` dýl. To je ta menší
škoda, jak jsi psal — obráceně by cache vracela `nil` pro entitu, kterou volající
vidí v databázi.

`markedForDeletion` se maže při každém úspěšném načtení (`updateEntry`) a při
každé rekonciliaci, která ID ve zdroji najde. Aby se při rekonciliaci nešpinila
cache line každé položky, čte se příznak nejdřív obyčejným `Load` a zapisuje se
jen tehdy, když je skutečně nastavený.

### Rate limit na misses

`Get` na neznámé ID zařadí ID do fronty, jak zadání chce. Bez tombstonů ale
neexistuje záporný záznam, takže by volající iterující náhodná ID udělal z každého
čtení dotaz do zdroje. Proto zůstal `MissRateLimit` (výchozí 100/s) — omezuje
**jen** tuhle cestu. `Invalidate`, TTL ani rekonciliace omezené nejsou, takže
skutečně nová položka se vždy dostane dovnitř.

Token bucket s mutexem se vyměnil za bezzámkové okno nad hrubými hodinami: miss
spadl ze 98,7 ns na 43,5 ns.

### Metriky

`reads_count` a `misses_count` počítá každý shard sám (`shard.reads`,
`shard.misses`, na cache line spolu s mutexem, který čtenář špiní tak jako tak) a
goroutina je raz za sekundu sečte do Promethea. Všechno ostatní se inkrementuje
přímo v místě události, protože nic z toho není na čtecí cestě.

Naměřeno: `Get` s registrovanými metrikami 6,4 ns/op vs. 6,3 ns/op bez nich.

## Co jsem rozhodl sám

| # | Rozhodnutí | Důvod |
|---|---|---|
| 1 | Zrušeno i `Sync`, `Reload`, `Stats`, `InvalidateMultiple` a `OnSync`. | „Jen `Get`, `Invalidate`, `Close`." Stav cache pokrývají metriky. |
| 2 | Místo poolu workerů **jedna** goroutina. | Zadání mluví o „nějaké další goroutině" v jednotném čísle a sekvenční běh ruší souběh se rekonciliací. |
| 3 | Seznam ID k načtení drží množina `pendingIDs`, příznak na položce je jen zkratka před jejím mutexem. | Kdyby byl příznak jediným zdrojem pravdy, musela by goroutina každou sekundu projít všech N položek. |
| 4 | TTL se randomizuje vždy minimálně o ±10 %, i když je `Randomizer` 0. | Jinak by všech 200 tis. položek nahraných úvodním načtením expirovalo v tutéž milisekundu. |
| 5 | Neúspěšné načtení nechá ID označená, příští tik je zkusí znovu. `ErrorRetryInterval` zrušen. | Tikot `RefreshInterval` sám je ta prodleva. |
| 6 | `toolchain go1.22.0` v `go.mod` **není**. | Go tooling ho jako duplicitní k `go 1.22.0` odmítá — s ním každý `go` příkaz hlásí „updates to go.mod needed". Direktiva `go 1.22.0` toolchain 1.22.0 vyžaduje sama. |

## Testy

`go test ./... -race` — vše zelené, opakovaně (`-count=3`).

Pokryté chování: dávkování úvodního načtení a jeho velikost, selhání
`ListIDsFunc`/dávky v `New`, validace parametrů, `store`/`remove`, absence
expirace, idempotentní `Close`, `Close` po zrušeném contextu, souběh čtení s
přenačítáním a rekonciliací, asynchronnost `Invalidate`, sloučení invalidací do
jedné dávky, deduplikace opakovaných invalidací, spuštění dávky při dosažení
`BatchSize`, mazání i načítání přes invalidaci, přežití hodnoty při neúspěšném
načtení a jeho zopakování, chyba u jedné položky vs. `ErrNotFound`, zařazení
neznámého ID z `Get`, rate limit misses, TTL (dál vrací hodnotu + označí),
přenačtení po TTL, ticho při `TTL = 0`, dvoufázové mazání ve všech třech
variantách (označit / smazat / odznačit), rekonciliace při chybě výpisu ID,
dávkování v rekonciliaci, recyklace bufferů rekonciliace, sharded metriky bez
dvojího počítání, metriky chyb a běh bez registru metrik.
