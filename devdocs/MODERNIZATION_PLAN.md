# План модернизации Metacrafter

Дата анализа: 2026-11-05. Состояние: metacrafter 0.0.6 (`b319103`), metacrafter-registry 1.3.0.dev0 (`e4540f7`), metacrafter-rules 0.0.2 (`1982330`).

## 1. Резюме

За год metacrafter-registry вырос с 630 до 1812 записей и стал платформой. В нём появились:

- v1 API с манифестом и SHA-256 для каждого файла;
- Python-пакет `metacrafter_registry.Registry` с методами `identify`, `match`, `resolve`, `crosswalk`;
- MCP-сервер, `llms.txt`, Markdown-версии страниц, RAG-корпус;
- алиасы и статусы `draft`/`stable`/`deprecated`;
- 49 алгоритмов контрольных сумм;
- негативные примеры (`valid: false`);
- кросс-маппинги на Presidio, Google DLP, DPV, Purview, Wikidata, Schema.org, FIBO, stdnum, FHIR и другие системы.

Metacrafter из всего этого использует только строковый шаблон URL `https://registry.apicrafter.io/datatype/{key}`.

Предлагаемое направление: registry становится единственным источником истины о семантике типов. Metacrafter становится движком детекции поверх registry: с новым форматом отчёта и интерфейсами для ИИ-агентов. metacrafter-rules остаётся знанием о том, *как* найти тип в данных. Целевая версия — **1.0.0**.

## 2. Текущее состояние: находки

### 2.1. Связь с registry

| Проблема | Где | Последствие |
|---|---|---|
| `BASE_URL` захардкожен | `metacrafter/classify/processor.py:221` | `registry_url` и `METACRAFTER_REGISTRY_URL` не работают, хотя `docs/docs/integrations/registry.md` это обещает. `ConfigLoader.get_registry_url()` нигде не вызывается. Registry просит это исправить (`modernize-registry-platform`, задача 8.7). |
| `RegistryClient` не используется | `metacrafter/registry/client.py` | Работает только с legacy-эндпоинтами `/registry.json` и `/datatype/<id>.json`. |
| Ключ правила выдаётся как id типа без резолва | `core.py:1422`, `RuleResult.class_url()` | 14 ключей указывают на алиасы (`ssn`→`usssn`, `guid`→`uuid`, `md5`→`md5hash` и др.). 32 ключа не существуют в registry: ссылки битые. |
| PII определяется только по `context: pii` файла правил | `processor.py:552` | Поле `is_pii` (425 правил) игнорируется: код сравнивает его со строкой `"True"`, а YAML даёт `bool`. Поле `is_pii` из registry не используется. |
| PII в интеграциях — зашитый список | `integrations/datahub.py:242` | `["email", "phone", "ssn", "passport", "creditcard"]` вместо 325 PII-типов registry. |
| Отчёт не обогащается | `core.py`, `scan_data` | Нет имени типа, категорий, статуса, маппингов, версии registry. |
| LLM-режим читает legacy JSONL по относительному пути | `classify/llm/registry_loader.py` | Негативные примеры встраиваются как позитивные. Индекс не инвалидируется при обновлении registry: в metacrafter-llm он на 407 записей из 1812. |

### 2.2. Движок детекции

- **Только три типа сопоставления:** `text`, `ppr` (pyparsing), `func`. Из registry нельзя использовать 1297 regexp: нет `match: regex`, а в безопасном пространстве имён pyparsing нет `Regex`. Алгоритмы контрольных сумм registry (49 штук, у 85 записей) тоже не применяются.
- **Уверенность — это только доля совпавших значений.** Не используются:
  - `priority`, заданный в 666 правилах;
  - комбинация «имя поля + значения»;
  - доля прохождения контрольной суммы;
  - специфичность паттерна.
- **Расхождения формата правил с движком:**
  - `validator` работает только у `ppr` data-правил;
  - `deprecated` игнорируется;
  - `minlen`/`maxlen` у `text`-правил перезаписываются;
  - `fieldrule` у field-правил игнорируется.
- **qddate-формат не связан с registry.** Нет `crosswalk("qddate", fmt)` для получения записи-паттерна вроде `date_dmy`.
- **Покрытие registry правилами — 27,7%:** 502 из 1812 записей. Из непокрытых 663 типа имеют `regexp` и примеры, 53 — контрольную сумму. Хуже всего покрыты категории:

  | Категория | Без правил |
  |---|---|
  | companies | 204/225 |
  | pii | 153/238 |
  | persons | 142/214 |
  | finances | 132/211 |
  | geo | 121/192 |
  | secrets | 67/71 |
- **Качество правил на примерах registry:**
  - recall по позитивам — 67% (385/572);
  - 11 негативов из 159 ошибочно приняты в 10 правилах: `ulidvalue`, `macaddressvalue`, `frvatvalue`, `de_pii_ibanvalue` и др.;
  - `isbn13` и `isbn10` не распознают формы с дефисами.

### 2.3. Правила

- **Встроенный `rules/` — устаревшая копия metacrafter-rules.** В нём 35 файлов и 140 правил. Из 138 общих правил 95 расходятся с актуальными.
- **Встроенные правила лежат вне пакета** и не попадают в wheel.
- **Они затеняют актуальные правила.** При запуске из checkout `DEFAULT_RULEPATH = ["rules"]` идёт первым и перекрывает актуальные правила. В логе 139 предупреждений «Duplicate rulekey».
- **Внутренний дубль:** `passportbyknown` есть и в `pii/pii.yaml`, и в `pii/ru/ru_pii.yaml`.

### 2.4. Код и инфраструктура

- **`core.py` — 2367 строк.** `CrafterCmd` — god object. Пакет `metacrafter/core/` подгружает `core.py` через `importlib`-хак.
- **pydantic v2 в зависимостях, но API v1.** В `config.py` используется `@validator`.
- **`requires-python >=3.8`.** Python 3.8 и 3.9 больше не поддерживаются, а registry требует `>=3.10`.
- **`requirements.txt` дублирует `pyproject.toml`** и тянет в runtime `pytest`, `openai`, `chromadb`. В основных зависимостях лежат `pymongo`, `duckdb`, `flask`, хотя нужны они не всем.
- **Тесты локально (Python 3.13): 219 passed, 22 failed, 50 skipped.**
  - LLM-тесты не изолированы от отсутствующих optional-зависимостей.
  - Падают XML-тесты (iterabledata), фильтр стран и тест дублей правил.
  - В CI `black`, `flake8` и `mypy` работают с `continue-on-error`.
- **HTTP-сервер на Flask с одним эндпоинтом** `/api/v1/scan_data`.
- **Репозиторий metacrafter-llm — устаревшая копия** `metacrafter/classify/llm`.
- **`openspec/ROADMAP.md` (июль 2026) устарел.** Он ссылается на 8 changes, из которых в `openspec/changes/` есть только `add-rules-optional-extra`. Часть пунктов уже выполнена (pyproject, extras), часть — нет.
- **Документация ссылается на несуществующее:** `scripts/generate_rule_skeletons.py` и модуль `metacrafter.rules.ru.gov`.

## 3. Целевая архитектура

### 3.1. Разделение ответственности

| Компонент | Отвечает за |
|---|---|
| **metacrafter-registry** | *Что* такое тип: id, имя, описание, PII, категории, страны, regexp, контрольная сумма, примеры, маппинги, статус. Единственный источник истины. |
| **metacrafter-rules** | *Как* найти тип в данных: эвристики имён полей, словари, специализированные валидаторы, приоритеты. Каждое правило ссылается на id registry. |
| **metacrafter** | Движок: источники данных, профилирование, детекция, скоринг, отчёт, экспорт, интерфейсы (CLI, Python API, HTTP, MCP). |

### 3.2. Структура пакета

```
metacrafter/
  registry/     # адаптер над metacrafter_registry.Registry: загрузка, кэш, резолв алиасов, обогащение
  rules/        # загрузка правил, JSON Schema формата, компиляция (text/ppr/func/regex/registry)
  engine/       # сопоставление, детекторы из registry, скоринг, qddate
  sources/      # file, sql, mongodb, duckdb, in-memory
  report/       # pydantic-модели отчёта, сериализация, legacy-формат
  exporters/    # datahub, openmetadata, atlas, crosswalk-экспорт, frictionless
  llm/          # кандидаты из registry + дисамбигуация LLM
  interfaces/   # cli (typer), http, mcp
```

`CrafterCmd` остаётся фасадом для обратной совместимости Python API.

### 3.3. Конвейер

1. **source** — чтение выборки.
2. **profile** — статистика полей (текущий `Analyzer`).
3. **candidates** — field-правила плюс сходство имени поля с именами и алиасами registry.
4. **value detection** — data-правила плюс regexp и контрольные суммы registry.
5. **scoring** — единая модель уверенности с разложением (`evidence`).
6. **resolve + enrich** — канонический id, метаданные и маппинги из registry.
7. **LLM** (опционально) — только для полей без уверенного результата.
8. **report** — отчёт v2 и экспорт.

## 4. Этапы

Оценки сроков даны для одного разработчика. Этап 5 идёт параллельно с этапами 1–4.

### Этап 0. Фундамент (1–2 недели)

OpenSpec change: `refactor-metacrafter-foundation`.

1. **Python ≥3.10.** CI-матрица 3.10–3.13. Обновить classifiers.
2. **pydantic v2 API** (`field_validator`, `model_config`).
3. **Зависимости.**
   - Удалить `requirements.txt` или генерировать его из `pyproject.toml`.
   - Вынести `pymongo`, `duckdb`/`duckdb-engine`, `flask` в extras: `mongodb`, `duckdb`, `server`.
4. **Тесты и линтеры.**
   - Optional-зависимости — через `pytest.importorskip`.
   - Починить XML-тесты, фильтр стран и тест дублей.
   - Линтер сделать блокирующим (`ruff` вместо `black` + `flake8`). `mypy` сделать блокирующим для новых модулей.
5. **Встроенные правила.**
   - Удалить устаревший `rules/` из репозитория.
   - Путь к правилам по умолчанию брать из пакета, а не из CWD.
   - Вариант решения — см. раздел 6, вопрос 2.
6. **Разделить `core.py`** на пакеты из раздела 3.2. Убрать `importlib`-хак. Публичный API: `CrafterCmd`, исключения.
7. **Обновить `openspec/ROADMAP.md`:** закрыть выполненное, сослаться на этот план.

**Критерии готовности:**
- CI зелёный на 3.10–3.13 и без optional-зависимостей, и со всеми extras;
- `pip install metacrafter` из wheel сканирует CSV вне checkout.

### Этап 1. Registry как источник истины (2–3 недели)

OpenSpec change: `add-registry-integration`.

1. **Зависимость `metacrafter-registry>=1.3`.** Адаптер `metacrafter.registry` ищет источник в порядке:
   1. локальный путь (`registry_path`, checkout или снапшот);
   2. кэш в `~/.cache`;
   3. `Registry.fetch()` с `/api/v1/` с проверкой SHA-256 по манифесту;
   4. снапшот, встроенный в пакет registry.

   Работа офлайн возможна всегда.
2. **Конфигурация:** `registry_url`, `registry_path`, `registry_offline`, переменная `METACRAFTER_REGISTRY_URL`. URL типа строится из `datatype_url_template` манифеста, а не из константы.
3. **Резолв ключей.** Ключ правила переводится в канонический id через `Registry.resolve()`. В отчёте:
   - `datatype` — канонический id;
   - `rule_key` — исходный ключ;
   - `replaced_by` — для `deprecated`.

   Неизвестный ключ даёт предупреждение. Новая команда `metacrafter rules check --registry` показывает битые ключи, алиасы и deprecated.
4. **Обогащение каждого совпадения:** `name`, `categories`, `is_pii`, `classification`, `status`, `country`, `url`. Плюс `mappings`: presidio, google_dlp, dpv, purview, schema_org, wikidata.
5. **PII.**
   - Итог: `is_pii` = registry `is_pii` ИЛИ `is_pii` правила ИЛИ `context: pii`.
   - Исправить сравнение `is_pii` с bool.
   - Пометка `draft` для неутверждённых типов.
6. **Фильтры на метаданных registry:** `--categories`, `--pii-only`, `--status stable`.
7. **Отчёт содержит `registry`:** `version`, `git_sha`, `generated_at`, `source`.
8. **`RegistryClient`** оставить как shim с `DeprecationWarning`.

**Критерии готовности:**
- URL в отчёте учитывают `registry_url`;
- ни одной ссылки на алиас;
- интеграционные тесты идут на снапшоте без сети.

### Этап 2. Движок детекции v2 (3–4 недели)

OpenSpec change: `update-detection-engine`.

1. **Новые типы сопоставления.**
   - `match: regex` — Python `re.fullmatch`, длина значения ограничена `maxlen`.
   - `match: registry` с `rule: <id>`. Детектор берёт regexp и контрольную сумму из записи registry. Правило добавляет только ограничения: `fieldrule`, `minlen`/`maxlen`, `priority`, `imprecise`.
   - `validator: checksum:<algo>` — использовать алгоритмы registry вместо дублирования в `metacrafterext`.
2. **Детекторы из registry** — флаг `--registry-detectors off|stable|all`.
   - Для записей с `regexp`, у которых нет правила, детекторы строятся автоматически.
   - По умолчанию: только `stable` с контрольной суммой или высокой специфичностью regexp.
   - Остальные помечаются `imprecise`.
   - Потенциал: до +663 типов.
3. **Единая модель уверенности 0–100.** Слагаемые:
   - доля совпавших значений × специфичность паттерна;
   - доля прохождения контрольной суммы;
   - совпадение имени поля (field-правила и сходство с именами и алиасами registry);
   - `priority` правила;
   - бонус страны (`--country` или контекст источника);
   - штраф за `draft`.

   Модель согласовать с `Registry.identify()`: вызывать его или вынести общий модуль скоринга. В отчёт пишется разложение `evidence`.
4. **Устранить расхождения формата правил:**
   - `is_pii` — bool;
   - `validator` — для всех типов сопоставления;
   - `deprecated` учитывается;
   - `fieldrule` работает у field-правил;
   - `minlen`/`maxlen` работают у `text`-правил;
   - `priority` учитывается.
5. **JSON Schema формата правил (v2).** Проверка при загрузке и командой `metacrafter rules validate`. Схема общая с metacrafter-rules.
6. **qddate.** `format` переводится через `crosswalk("qddate", fmt)` в id записи-паттерна (например, `date_dmy`).
7. **Производительность.**
   - Компилировать правила один раз.
   - Кэшировать скомпилированный набор по хэшу rulepath и версии registry.
   - Отсекать правила по длине и типу до прогона значений.
8. **Packs.** `--pack <id>`, после `add-datatype-packs` в registry.

**Критерии готовности:**
- recall на позитивных примерах registry ≥ 90% для покрытых типов;
- 0 принятых негативов для типов с контрольной суммой;
- скорость на эталонном наборе не хуже текущей.

### Этап 3. Отчёт v2 и экспорт (2 недели)

OpenSpec change: `add-report-v2`.

1. **Типизированный отчёт.** Pydantic-модели: `ScanReport`, `FieldReport`, `Match`, `Evidence`, `RegistryRef`.
   - JSON Schema отчёта публикуется на сайте документации.
   - Поле `report_format: 2`.
   - `--format legacy` выдаёт старый формат на 1–2 минорных релиза.
2. **Каталоги** (DataHub, OpenMetadata, Atlas): PII и glossary terms берутся из registry, со ссылкой на registry URL. Маппинги идут как custom properties. Захардкоженные списки типов удаляются.
3. **Crosswalk-экспорт:** сущности Presidio, infoTypes Google DLP, DPV, Purview — через `mappings`.
4. **Frictionless Table Schema / CSVW** с семантическими аннотациями (`rdfType` из Wikidata или Schema.org).
5. **`--format md`** — Markdown-отчёт для людей и агентов.
6. **`metacrafter scan diff`** — сравнение двух отчётов.

**Критерии готовности:**
- отчёт валиден по опубликованной схеме;
- в коде экспортёров нет списков типов.

### Этап 4. ИИ-агенты и LLM (3 недели)

OpenSpec changes: `add-agent-interfaces`, `update-llm-classification`.

1. **MCP-сервер `metacrafter mcp`** (`stdio` и `streamable-http`).
   - Инструменты:
     - `classify_columns(columns=[{name, values[]}])`;
     - `scan_file(path, limit)`;
     - `scan_sql(url, tables, limit)` — только чтение, с лимитами и allowlist путей и соединений;
     - `explain_match(field, datatype)`;
     - `list_rules(filter)`.
   - id и URL согласованы с MCP-сервером registry: агент может вызывать оба.
2. **Skills:**
   - `skills/metacrafter-scan/SKILL.md` — как сканировать и интерпретировать результат;
   - `skills/metacrafter-rule-author/SKILL.md` — как написать правило по записи registry и проверить его на примерах.
3. **HTTP API.**
   - OpenAPI.
   - Эндпоинты `/v2/classify`, `/v2/scan`, `/health`.
   - `/api/v1/scan_data` сохраняется.
   - Фреймворк — см. раздел 6, вопрос 5.
4. **Документация для агентов:** `llms.txt` и Markdown-версии страниц на сайте документации.
5. **LLM v2.**
   - **Кандидаты** выбираются детерминированно: `Registry.identify()` и `search()`, top-k. Эмбеддинги необязательны. ChromaDB становится опциональным бэкендом: 1812 записей помещаются в память.
   - **Корпус** — `/api/v1/rag/corpus.jsonl` или пакет registry. Индекс пересобирается при смене SHA-256 в манифесте.
   - **Негативные примеры** подаются явно как «не является этим типом».
   - **Ответ** — structured output или tool call со схемой. id проверяется по registry с резолвом алиасов; несуществующие id отклоняются.
   - **В гибридном режиме** LLM вызывается только для полей без уверенного результата. Кэш по ключу (имя поля, хэш значений, версия registry).
   - **Провайдеры:** один OpenAI-совместимый клиент (покрывает OpenAI, OpenRouter, Ollama, LM Studio, Perplexity) плюс Anthropic. Пять дублирующихся классов провайдеров удаляются.
   - **Защита PII.** По умолчанию сырые значения не уходят во внешние LLM. Варианты: маскирование, только имена полей и статистика, или только локальные провайдеры.
6. **Архивировать репозиторий metacrafter-llm.**

**Критерии готовности:**
- агент через MCP размечает таблицу и получает канонические id с URL;
- результаты LLM не содержат id вне registry.

### Этап 5. Качество и экосистема (параллельно)

OpenSpec change: `add-ecosystem-contract-tests` (заменяет `add-ecosystem-alignment-workflow` из старого ROADMAP).

1. **Контрактный тест:** все ключи правил резолвятся в registry. Сейчас битых ключей 32. Тест работает в CI metacrafter и metacrafter-rules.
2. **Тесты правил на примерах registry.**
   - Команда `metacrafter rules test --registry-examples` прогоняет каждое правило по позитивам и негативам и выдаёт recall и precision.
   - Основа — скрипт из этого анализа.
   - Порог — гейт CI в metacrafter-rules.
3. **`ecosystem.lock` в metacrafter:** версии registry и rules, с которыми проверен релиз. Еженедельный workflow проверяет совместимость с HEAD соседних репозиториев.
4. **Бенчмарк** — размеченные датасеты для регрессий качества и скорости.
5. **Документация.**
   - Обновить Docusaurus: registry, MCP, отчёт v2, гайд миграции 0.x → 1.0.
   - Убрать устаревшие devdocs и ссылки на несуществующее.

## 5. Что нужно от соседних репозиториев

**metacrafter-registry:**
- опубликовать пакет на PyPI (действие владельца: trusted publisher);
- выпустить v2.0.0 (задачи openspec 9.x);
- вынести `Jinja2` и прочие зависимости сборки сайта в extra `build`, чтобы runtime-зависимость была лёгкой;
- реализовать `add-datatype-packs` (`/api/v1/packs/<id>.json`);
- опубликовать метрику специфичности regexp (`broadness`) в API.

**metacrafter-rules:**
- исправить 32 битых ключа: 31 обобщённое field-правило вида `<token>_<category>` в `rules/common/*.yaml` и `ca_sin` → `casin`. Альтернатива — добавить алиасы в registry;
- перевести правила на JSON Schema формата v2;
- добавить поддержку `match: regex` и `match: registry`;
- перевести `setup.py` на `pyproject.toml`;
- починить 10 падающих тестов в `tests/test_identifiers_validation.py`;
- закоммитить `.github/workflows/ci.yml` (сейчас не отслеживается git);
- переиспользовать алгоритмы контрольных сумм registry вместо дублей в `metacrafterext`.

## 6. Решения, требующие выбора

1. **Registry — обязательная или опциональная зависимость?**
   Рекомендация: обязательная, после публикации на PyPI и выноса `Jinja2` в extra. Альтернатива: extra `registry` плюс HTTP-fallback на `/api/v1/`.
2. **Что делать со встроенными `rules/`?**
   Рекомендация: удалить, metacrafter-rules сделать обязательной зависимостью. Альтернатива: минимальный core-набор внутри пакета как package data, загружаемый *после* расширенного.
3. **Детекторы из registry по умолчанию?**
   Рекомендация: `stable`. Альтернативы: `off` или `all`.
4. **Python ≥3.10 и breaking-релиз 1.0.0 с отчётом v2?**
   Рекомендация: да, legacy-формат остаётся на переходный период.
5. **Flask → FastAPI для HTTP API?**
   Рекомендация: да, ради OpenAPI и единой валидации на pydantic.
6. **Архивировать metacrafter-llm?**
   Рекомендация: да.

## 7. Релизы

| Версия | Состав |
|---|---|
| 0.1.0 | Этап 0 |
| 0.2.0 | Этап 1 + контрактные тесты |
| 0.3.0 | Этап 2 + тесты правил на примерах |
| 1.0.0 | Этапы 3–4, отчёт v2 по умолчанию |

## 8. Риски

| Риск | Мера |
|---|---|
| Пакет registry не опубликован на PyPI — это блокирует этап 1 | Fallback: HTTP-загрузка v1 API с проверкой SHA-256 и кэшем |
| 76% записей registry — `draft`; автодетекторы на них дадут ложные срабатывания | Гейтинг по статусу и специфичности, пометка `imprecise`, флаг `draft` в отчёте |
| ReDoS на regexp из registry | Ограничение длины значения; проверка regexp в CI registry; опционально пакет `regex` с таймаутом |
| Ломка формата отчёта у пользователей | `--format legacy`, гайд миграции, SemVer |
| Утечка PII во внешние LLM | По умолчанию значения не отправляются; маскирование; локальные провайдеры |
| Расхождение версий трёх репозиториев | `ecosystem.lock`, контрактные тесты, еженедельная проверка |

## 9. Метрики успеха

| Метрика | Сейчас | Цель 1.0 |
|---|---|---|
| Записи registry с детектором | 27,7% (502/1812) | ≥ 60% |
| Битые ключи правил | 32 | 0 (гейт CI) |
| Recall на позитивных примерах registry | 67% | ≥ 90% |
| Принятые негативные примеры | 11/159 | 0 для типов с контрольной суммой |
| PII-типы, известные экспорту | 5 (хардкод) | 325 (из registry) |
| Тесты | 22 падают локально | зелёные на 3.10–3.13 |

## 10. Следующие шаги

1. Принять решения из раздела 6.
2. Оформить OpenSpec-предложения по этапам 0–5 в `openspec/changes/` с `openspec validate --strict`.
3. Завести задачи в `bd`.
4. Параллельно запросить у владельца registry публикацию на PyPI. Это критический путь этапа 1.
