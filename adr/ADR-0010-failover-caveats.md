# ADR-0010: Ограничения и риски failover

**Статус:** Предложено
**Дата:** 2026-11-18

# Context

[ADR-0007](ADR-0007-safe-failover.md) описывает failover v1 для одного timeline,
а [ADR-0011](ADR-0011-safe-failover-two-timelines.md) — failover v2. Их
доказательства сохранности данных опираются на предпосылки PostgreSQL и на
точное состояние cluster state. Этот документ собирает ограничения и принятые
риски обоих алгоритмов. Кворумная probe из
[ADR-0009](ADR-0009-failover-probe.md) относится только к v2.

# Decision

## Границы гарантии

Гарантия требует `fsync = on`, синхронного коммита с ожиданием flush, уникальных
`application_name` реплик и линеаризуемых CAS-операций ZooKeeper. Внешнее
изменение SSN, источников WAL или состояния failover в гарантии не входит.
Ручное разрешение потери данных также не входит в гарантии алгоритмов failover.

Ошибку `lwaldump_with_timeline()` или разбора history нельзя заменять receive
или replay LSN: после рестарта receive LSN недоступен, а replay LSN может
отставать от уже сохранённого локального WAL. Надёжного признака завершения
replay локального WAL нет. Невалидный голос может заблокировать автоматический
выбор, но не ослабляет правило сохранности данных.

Противоречивые history и возможные подтверждённые commit на несовместимых
ветках также блокируют выбор. Числовой порядок LSN разных timeline не
доказывает, что один хост содержит WAL другого.

## Archive restore не входит в WAL-fence

Перед голосованием реплика best effort отключает archive restore. Для этого
pgconsul генерирует UUID и выставляет команду:

```conf
restore_command = 'echo <uuid> > <marker-path>.tmp && mv -f <marker-path>.tmp <marker-path>; false'
```

Один marker-файл используется только одним PostgreSQL-инстансом. Временный файл
находится рядом с ним, а атомарный rename заменяет marker предыдущей попытки без
промежутка между удалением и созданием. Завершающий `false` сообщает PostgreSQL,
что запрошенного файла нет. Совпадение UUID означает, что startup process вызвал
новую команду после завершения предыдущего restore.

Pgconsul ждёт marker не дольше `archive_restore_disable_timeout` из секции
`[replica]`; значение по умолчанию — 5 секунд. При таймауте голосование
продолжается: startup process мог не обработать SIGHUP или ещё проигрывать
локальный WAL. Ожидание не входит в WAL-fence и лишь уменьшает вероятность
наката за switchpoint и последующего rewind.

WAL или history, полученные до сканирования, учитываются в голосе. Полученные
после него не делают более ранний снимок небезопасным: архив сам по себе не
подтверждает синхронный коммит. Уже запущенный restore всё же может добавить
history-файл и повлиять на фактический timeline последующего promotion.

## Sender старого timeline может подтвердить commit нового timeline

До promotion кандидат мог получить и отправить другой реплике WAL прежнего
timeline дальше будущего switchpoint. После promotion числовой LSN такого
подтверждения может быть больше LSN новой транзакции. Sync-rep PostgreSQL
сопоставляет ожидание commit с write/flush/apply LSN sender'а, не проверяя
timeline подтверждения.

Поэтому sender старого timeline может освободить ожидание commit нового
timeline, хотя WAL новой транзакции не попал на подтверждающую реплику.
Применение целевого SSN до promotion не закрывает гонку. Требуется изменение
PostgreSQL либо наблюдаемое подтверждение timeline каждого sender'а. Риск пока
принимается.

## Фактический timeline может отличаться от разрешённого

С патчем `target promote` PostgreSQL получает сохранённый timeline как явную
цель. Без патча pgconsul рассчитывает его как PostgreSQL 14:
`findNewestTimeLine(recoveryTargetTLI) + 1`. Кандидат не возобновляет archive
restore и walreceiver до promotion, но уже запущенный restore может добавить
history-файл и изменить фактический timeline.

Без `target promote` фактический timeline может отличаться от записанного в
последней конфигурации. Клиентская запись после promotion не ждёт CAS, которым
новый primary исправляет timeline. Если primary подтвердил commit и отказал в
этом окне, следующий failover увидит устаревший timeline и не сможет корректно
классифицировать фактическую ветку. Риск пока принимается. Если primary остаётся
доступен, CAS повторяется в фазе `promoting` до сохранения фактического timeline.

## Применение SSN

Гонки `SYNC_STANDBY_DEFINED` и смешанных конфигураций sender'ов описаны в
[ADR-0006](ADR-0006-safe-durability-membership-change.md). Они могут нарушить
предположение, что commit и WAL-барьер подтверждены write quorum одной из
сохранённых конфигураций. Этот риск также принимается.

## Последовательные отказы и доступность

Входной failover допускает не более двух различных timeline в
`possible_write_configurations`. Разрешение на replica-promotion может добавить
третий. Если победитель откажет до WAL-барьера, в том числе до фактического
promotion, следующая автоматическая попытка не начнётся: алгоритм не умеет
обработать три входных timeline. Все конфигурации остаются для ручного
восстановления; забывать конфигурацию, на которой могли подтверждаться
транзакции, нельзя.

Живой зависший держатель failover-manager lock может задержать восстановление
до истечения ZooKeeper-сессии. Недостаток валидных голосов также может
остановить автоматический выбор.

Размер списка `possible_write_configurations` ограничен максимальным размером
znode ZooKeeper/ClickHouse Keeper. Если новая запись не помещается,
автоматический failover останавливается: старую конфигурацию нельзя безопасно
удалить без WAL-барьера.

# Alternatives

| Вариант | Почему не выбран |
|---|---|
| Начинать failover с произвольным числом timeline и строить общее дерево history | Это повысило бы доступность при последовательных отказах до WAL-барьера, но усложнило бы классификацию веток, проекции голосов и recovery. |
| Считать archive restore частью обязательного WAL-fence | Startup process может ещё проигрывать локальный WAL или не обработать SIGHUP в заданный срок; архив сам по себе не подтверждает синхронный commit. |
| Заменить ошибочный голос receive или replay LSN | Эти позиции не доказывают сохранность всего локального WAL после рестарта. |

# Consequences

- Доказательства сохранности данных ADR-0007 и ADR-0011 условны относительно
  перечисленных предпосылок и принятых рисков PostgreSQL.
- Ошибки измерения, history и ограничения протокола могут остановить
  автоматический failover, сохранив прежние возможные write-конфигурации.
- При последовательных отказах до WAL-барьера может потребоваться ручное
  восстановление.

# Links

- [ADR-0005](ADR-0005-cluster-state-and-primary-authority.md) — cluster state и разрешение на primary
- [ADR-0006](ADR-0006-safe-durability-membership-change.md) — WAL-барьер и риски SSN
- [ADR-0007](ADR-0007-safe-failover.md) — базовый failover для одного timeline
- [ADR-0009](ADR-0009-failover-probe.md) — обычный автоматический вход в failover v2
- [ADR-0011](ADR-0011-safe-failover-two-timelines.md) — алгоритм failover v2
- [PostgreSQL 14: walreceiver](https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/replication/walreceiver.c) — получение history до стриминга WAL
- [PostgreSQL 14: xlog](https://github.com/postgres/postgres/blob/REL_14_STABLE/src/backend/access/transam/xlog.c) — выбор нового timeline при promotion
