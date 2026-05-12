# Split pythonpkg import vs diploma work — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Разнести изменения на два PR в `prawwtocol/otterbrix`: PR1 — байт-в-байт перенос деревьев `pythonpkg/integration/python` и `pythonpkg/core` с зафиксированным `HEAD` локального `pythonpkg`; PR2 — всё остальное до целевого состояния дипломной ветки (сборка, инклюды, ядро, бенчи, CI).

**Architecture:** Ветка импорта создаётся от чистой базы `origin/main` в клоне `otterbrix`, затем `rsync` из локального `pythonpkg@<hash>` в те же относительные пути (без правок файлов после копирования). Вторая ветка — от вершины импорта; рабочее дерево выравнивается под известную финальную ветку через `git checkout <финальная-ветка> -- .`, затем коммит(ы) дают дифф «интеграция + доработки».

**Tech Stack:** Git, `rsync` (macOS), при необходимости `diff`/`cmp`; локальные пути ниже приведены для машины разработчика `diploma33`.

**Константы планируемого прогона (замените при расхождении):**

- `OTTERBRIX=/Users/seliverstow/Desktop/diploma33/otterbrix`
- `PYTHONPKG=/Users/seliverstow/Desktop/diploma33/pythonpkg`
- `PYTHONPKG_REV=deefc3404994fbe964f9f3a4bbbb0b2571e54799` (проверить перед PR: `git -C "$PYTHONPKG" rev-parse HEAD`)
- `IMPORT_BRANCH=import/pythonpkg-deefc34`
- `BASE_REMOTE=origin` и ветка базы: `main` (если в форке целевая база другая — заменить во всех командах)
- `FINAL_BRANCH=feat/python-integration-authored-1` — ветка с полным желаемым состоянием после диплома (текущая рабочая; переименуйте при необходимости)
- `FORK_REMOTE=fork` — remote на `prawwtocol/otterbrix` (если имя другое — подставить)

---

## Карта файлов (граница PR1)

| Источник (`pythonpkg`) | Назначение (`otterbrix`) | Правило |
|------------------------|---------------------------|---------|
| `integration/python/**` | `integration/python/**` | Для каждого файла, существующего слева, содержимое справа после PR1 идентично слева (после исключений ниже). |
| `core/**` | `core/**` | То же: только пути, которые есть в `pythonpkg/core`, копируются поверх; файлы только в `otterbrix` не удаляются. |

**Исключения при копировании (не часть «исходного» смысла, только гигиена репозитория):**

- Каталоги `__pycache__/` и файлы `*.pyc` не копировать.

Остальные пути в PR1 **не трогать** (нет массового удаления чужих файлов `otterbrix`).

---

### Task 1: Зафиксировать ревизию `pythonpkg` и убедиться в чистоте рабочей копии

**Files:** нет

- [ ] **Step 1: Записать hash и проверить отсутствие незакоммиченных изменений в `pythonpkg`**

```bash
git -C "$PYTHONPKG" rev-parse HEAD
git -C "$PYTHONPKG" status -sb
```

Ожидаемо: вывод hash совпадает с `PYTHONPKG_REV`, статус чистый (`## main` или иная ветка без `M`/`??` для отслеживаемых файлов).

- [ ] **Step 2: Если hash другой — обновить константу**

Подставить новый полный hash в описание PR1, в сообщение коммита Task 3 и в имя ветки `IMPORT_BRANCH` (короткий суффикс).

---

### Task 2: Создать ветку импорта от базы `otterbrix`

**Files:** нет (только ссылки Git)

- [ ] **Step 1: Обновить `main` и ответвиться**

```bash
cd "$OTTERBRIX"
git fetch "$BASE_REMOTE" main
git checkout -b "$IMPORT_BRANCH" "$BASE_REMOTE/main"
```

Ожидаемо: `git status -sb` показывает `## $IMPORT_BRANCH` без локальных правок.

---

### Task 3: Механический перенос из `pythonpkg` (PR1)

**Files:**

- Затронуты все файлы под `integration/python/**` и те под `core/**`, для которых есть источник в `pythonpkg`; добавляются новые, обновляются совпадающие пути.

- [ ] **Step 1: Выровнять деревья `rsync` (без последующих правок содержимого)**

```bash
cd "$OTTERBRIX"
mkdir -p integration/python core
rsync -a --delete --exclude '__pycache__' --exclude '*.pyc' \
  "$PYTHONPKG/integration/python/" "$OTTERBRIX/integration/python/"
rsync -a --exclude '__pycache__' --exclude '*.pyc' \
  "$PYTHONPKG/core/" "$OTTERBRIX/core/"
```

Замечание: `--delete` на `integration/python` удаляет в целевом каталоге файлы, которых нет в источнике (чтобы копия ветки без ручных удалений совпадала с составом `pythonpkg`). Это **не** удаляет файлы `otterbrix` вне `integration/python`. Для `core` `--delete` намеренно **не** используется: сохраняются файлы ядра `otterbrix`, отсутствующие в узком дереве `pythonpkg/core`.

- [ ] **Step 2: Проиндексировать и закоммитить**

```bash
git add -A integration/python core
git status
git commit -m "import: mechanical sync from pythonpkg integration/python and core

Upstream: pythonpkg@$PYTHONPKG_REV"
```

Подставить реальный hash вместо переменной в сообщении, если shell не раскрыл (проще одной строкой: скопировать из `git -C "$PYTHONPKG" rev-parse HEAD`).

Ожидаемо: один коммит на ветке `$IMPORT_BRANCH`; **нет** правок CMake в корне, `conanfile`, `.github`, `components/`, `services/` — только затронутые пути выше.

---

### Task 4: Проверка PR1 против `pythonpkg`

**Files:** нет

- [ ] **Step 1: Сравнить деревья (должны совпадать с учётом исключений)**

```bash
diff -rq --exclude '__pycache__' --exclude '*.pyc' \
  "$PYTHONPKG/integration/python" "$OTTERBRIX/integration/python"
```

Ожидаемо: пустой вывод (нет отличий).

```bash
# Для core: отличия «Only in otterbrix» допустимы; не должно быть отличий в файлах с одинаковым относительным путём
for f in $(cd "$PYTHONPKG/core" && find . -type f \( ! -path './__pycache__/*' ! -name '*.pyc' \)); do
  cmp -s "$PYTHONPKG/core/$f" "$OTTERBRIX/core/$f" || echo "MISMATCH $f"
done
```

Ожидаемо: нет строк `MISMATCH`.

- [ ] **Step 2: Убедиться, что сборка не входит в критерии PR1**

По спецификации не требуется запуск `cmake` / CI; при желании локально можно убедиться, что конфигурация падает — и упомянуть в описании PR на GitHub.

---

### Task 5: Открыть PR1 в форке

**Files:** при необходimости `.github` позже — только в PR2

- [ ] **Step 1: Запушить ветку импорта**

```bash
cd "$OTTERBRIX"
git push -u "$FORK_REMOTE" "$IMPORT_BRANCH"
```

- [ ] **Step 2: Создать Pull Request** в веб-интерфейсе GitHub: base — нужная ветка форка (часто `main`), compare — `$IMPORT_BRANCH`.

Текст для описания PR1 (вставить hash явно):

```text
Механический перенос из локального pythonpkg@<ПОЛНЫЙ_HASH>:
- rsync integration/python → integration/python (с удалением лишних путей только внутри этой директории);
- rsync core → core без --delete (файлы только otterbrix сохраняются).

Исключены __pycache__ и *.pyc.

Ожидаемо: проект не обязан собираться; правки инклюдов/CMake/ядра — в следующем PR.
```

---

### Task 6: Ветка PR2 — полное состояние диплома поверх импорта

**Files:** потенциально всё дерево, которое отличается от `$IMPORT_BRANCH` в финальной ветке (интеграция, ядро, бенчи, CI и т.д.)

- [ ] **Step 1: Обновить ссылку на финальную ветку и создать рабочую ветку от импорта**

```bash
cd "$OTTERBRIX"
git fetch "$FORK_REMOTE" "$FINAL_BRANCH" 2>/dev/null || true
git checkout "$IMPORT_BRANCH"
git pull "$FORK_REMOTE" "$IMPORT_BRANCH"
git checkout -b feat/diploma-on-import "$IMPORT_BRANCH"
```

- [ ] **Step 2: Выровнять индекс под финальное дерево**

```bash
git checkout "$FINAL_BRANCH" -- .
git status
```

Ожидаемо: список изменённых/добавленных/удалённых файлов отражает весь объём работ от механического импорта до диплома.

- [ ] **Step 3: Закоммитить PR2 (минимум один коммит; при желании разбить интерактивно)**

Минимальный вариант — один коммит:

```bash
git add -A
git commit -m "feat: integrate pythonpkg tree into otterbrix (build, core, benches)"
```

Либо перед коммитом выполнить `git reset -p` / разделение через `git add -p` по логическим частям.

- [ ] **Step 4: Проверка, что рабочее дерево совпадает с финальной веткой**

```bash
git diff "$FINAL_BRANCH" --exit-code
```

Ожидаемо: **нет вывода**, код возврата `0`. Если есть отличия — вернуться к `git checkout "$FINAL_BRANCH" -- .` и повторить индексацию.

- [ ] **Step 5: Отправить ветку и открыть PR2**

```bash
git push -u "$FORK_REMOTE" feat/diploma-on-import
```

Base в GitHub для PR2: **ветка, в которую смержили PR1** (или та же `$IMPORT_BRANCH`, если merge ещё нет — тогда base указывает на неё, а после merge перебазировать; предпочтительно открыть PR2 после merge PR1, base = `main` форка).

---

## Проверка покрытия spec (self-review)

| Требование spec | Где в плане |
|-----------------|------------|
| PR только механический, без правок содержимого после копирования | Task 3: `rsync` + один коммит без ручных правок |
| Hash `pythonpkg` зафиксирован | Task 1, сообщение коммита Task 3, текст PR1 |
| Сборка PR1 не обязана проходить | Task 4 Step 2, текст PR1 |
| PR2 — вся интеграция и доработки | Task 6 |
| Проверка соответствия снимку | Task 4 |

---

## Execution handoff

**План сохранён в** `docs/superpowers/plans/2026-05-12-split-pythonpkg-two-prs.md`.

**Два варианта выполнения:**

1. **Subagent-Driven (рекомендуется)** — отдельный агент на каждую задачу, ревью между задачами.

2. **Inline execution** — последовательное выполнение шагов в одной сессии с контрольными точками.

Какой вариант вам удобнее?
