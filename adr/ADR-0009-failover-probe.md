# ADR-0009: Кворумная проба недоступности primary для failover v2

**Статус:** Принято
**Дата:** 2026-11-18

# Context

Автоматический failover нельзя начинать по наблюдению одной реплики: она может
оказаться в сетевой партиции, пока primary продолжает подтверждать транзакции.
Состав durability members и разрешение на primary хранятся в cluster state по
[ADR-0005](ADR-0005-cluster-state-and-primary-authority.md). В незавершённой
операции там может быть несколько `possible_write_configurations`.

**Probe** — ограниченная по времени попытка доказать недоступность текущего
`desired_primary`. Её идентификатор равен `state_id` проверяемого cluster state.
**Ответ probe** — свежая запись реплики с этим идентификатором, подтверждающая
недоступность `desired_primary` именно из проверяемой ревизии.

Эта ADR является частью алгоритма failover v2 из
[ADR-0011](ADR-0011-safe-failover-two-timelines.md) и определяет только его
обычный автоматический вход в `failover/voting`. Failover v1 из
[ADR-0007](ADR-0007-safe-failover.md) запускается без probe по собственным
правилам. Переход v2 из незавершённого switchover определён отдельно в
[ADR-0008](ADR-0008-safe-switchover.md).

# Decision

## Состояние probe

Пути указаны относительно cluster prefix:

| Путь | Назначение |
|---|---|
| `/failover_probe` | Идентификатор probe, равный проверяемому `state_id`. |
| `/failover_health/<fqdn>` | Ответ probe; свежесть определяется по mtime. |
| `/failover_manager` | Ephemeral-блокировка координатора, выполняющего переход в `failover/voting`. |

Probe хранится отдельно от cluster state и не блокирует обычную смену durability
members. Реплика читает `desired_primary` и `possible_write_configurations` из
ревизии probe, проверяет недоступность именно этого primary и публикует ответ с
идентификатором probe.

## Кворум недоступности

Для каждой конфигурации `(D, P, T)` требуется `Q(D)` свежих ответов от
`R(D, P)`. Учитываются только ответы с текущим идентификатором, чья mtime входит
в срок жизни probe. Любой CAS cluster state меняет `state_id` и инвалидирует
probe со всеми ответами.

Если список конфигураций не инициализирован или в нём нет реплик, безопасный
автоматический failover невозможен. Если список содержит больше двух различных
timeline, probe не может перейти в failover независимо от числа ответов.

Probe quorum доказывает основание начать failover, но не заменяет WAL-fence и
election read quorum. Эти два условия проверяются уже внутри алгоритма
[ADR-0011](ADR-0011-safe-failover-two-timelines.md).

## Атомарный переход к failover

Первый хост, увидевший все probe quorum, получает failover-manager lock,
повторно читает probe, ответы и cluster state и одним CAS:

1. проверяет неизменность `state_id`, непустой `desired_primary` и не более двух
   различных timeline в `possible_write_configurations`;
2. сохраняет текущий `desired_primary` как `previous_primary`;
3. фиксирует electorate как объединение `D` всех возможных конфигураций;
4. устанавливает `desired_primary = None`;
5. переводит операцию в `failover/voting`, не меняя
   `possible_write_configurations`;
6. записывает новый `state_id`.

Успешный CAS является единственной границей начала failover и исключением из
правила, по которому вне failover cluster state меняет только
`desired_primary`. До него primary может продолжать reconciliation; конфликт
CAS инвалидирует probe и запускает новую попытку для свежего состояния.

Если новый primary предыдущего failover отказал до WAL-барьера и в списке
осталось не более двух timeline, CAS новой попытки заменяет метаданные failover,
но сохраняет весь список возможных конфигураций. При трёх timeline новая
попытка не начинается.

После успешного CAS действует алгоритм
[ADR-0011](ADR-0011-safe-failover-two-timelines.md).
Переход незавершённого switchover в ту же фазу происходит без probe по
[ADR-0008](ADR-0008-safe-switchover.md).

# Alternatives

| Вариант | Почему отвергнут |
|---|---|
| Записывать probe в cluster state | Одна изолированная реплика могла бы остановить смену durability members до кворумного доказательства отказа. |
| Не привязывать probe к `state_id` | Ответы могли бы относиться к уже изменившимся write-конфигурациям. |
| Начинать failover по ответу одной реплики | Изолированная реплика может потерять связь с доступным primary. |

# Consequences

- Probe одной реплики не блокирует primary; failover начинается только после
  кворумного доказательства и CAS.
- Изменение cluster state инвалидирует probe и все её ответы.
- Probe даёт кворум наблюдений недоступности прежнего primary, но невозможность
  нового commit доказывается только последующим WAL-fence из ADR-0011.
  Продолжающийся reconciliation не должен бесконечно инвалидировать probe; если
  это происходит, страдает доступность, но не сохранность данных.
- Ограничения последовательных failover перечислены в
  [ADR-0010](ADR-0010-failover-caveats.md).

# Links

- [ADR-0005](ADR-0005-cluster-state-and-primary-authority.md) — cluster state и определения кворумов
- [ADR-0007](ADR-0007-safe-failover.md) — отдельный алгоритм failover v1 без probe
- [ADR-0008](ADR-0008-safe-switchover.md) — вход из незавершённого switchover
- [ADR-0010](ADR-0010-failover-caveats.md) — ограничения и принятые риски
- [ADR-0011](ADR-0011-safe-failover-two-timelines.md) — алгоритм failover v2
- [Структура ZooKeeper](../docs/ZK.md)
