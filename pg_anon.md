---
Help Center Version: '1.0'
ajs-base-url: 'https://wiki.astralinux.ru'
ajs-enabled-dark-features: 'enable.legacy.edit.in.office,site-wide.shared-drafts,site-wide.synchrony,clc.quick.create,confluence.view.edit.transition,cql.search.screen,confluence-inline-comments-resolved,frontend.editor.v4,http.session.registrar,nps.survey.inline.dialog,confluence.efi.onboarding.new.templates,frontend.editor.v4.compatibility,atlassian.cdn.static.assets,pdf-preview,previews.sharing,previews.versions,file-annotations,confluence.efi.onboarding.rich.space.content,collaborative-audit-log,confluence.reindex.improvements,previews.conversion-service,editor.ajax.save,read.only.mode,graphql,previews.trigger-all-file-types,attachment.extracted.text.extractor,lucene.caching.filter,confluence.table.resizable,notification.batch,previews.sharing.pushstate,confluence-inline-comments-rich-editor,tc.tacca.dacca,site-wide.synchrony.opt-in,file-annotations.likes,gatekeeper-ui-v2,v2.content.name.searcher,mobile.supported.version,pulp,confluence-inline-comments,confluence-inline-comments-dangling-comment,quick-reload-inline-comments-flags'
ajs-page-id: 238755875
ajs-parent-page-id: 238754188
ajs-space-key: TANDOCS
ajs-static-resource-url-prefix: '/s/-gnfe4v/8703/98yf4s/\_'
ajs-user-locale: 'ru\_RU'
atl_token: 78a67e8a5a876c825fcdeaee5010bdcffa2c979a
atlassian-token: 78a67e8a5a876c825fcdeaee5010bdcffa2c979a
confluence-base-url: 'https://wiki.astralinux.ru'
generator: 'Created with Scroll Viewport - K15t'
lang: en
scroll-viewport-version: '2.20.4'
scrollSearch: false
theme-base-url: '/tandocs/\_/0A7082080184CD60780AA7440F7F88EF/1669895644691'
title: Инструмент поиска и маскирования конфиденциальных данных
viewport: 'width=device-width, initial-scale=1'
---

::: {.sp-viewport-control-container style="box-shadow: -2px 0 3px rgba(0, 0, 0, 0.5); display: block; position: fixed; top: 0; left: -21em; height: 100%; width: 20em; z-index: 100001; transition: left 0.5s ease-in-out"}
:::

::: {#sp-viewport-control-opener style="box-sizing: content-box; position: fixed; background-color: #0065ff; padding: 8px 10px; display: flex; align-items:center; zoom: 1.005; overflow: hidden; top: calc(50% + 64px); left: -98px; text-align: center; cursor: pointer; z-index: 100000; -moz-transform: rotate(90deg); -o-transform: rotate(90deg); -webkit-transform: rotate(90deg); -moz-transform-origin: 100% 0; -o-transform-origin: 100% 0; -webkit-transform-origin: 100% 0;"}
[Viewport
Control]{style="color: white; font-size: 12px; font-family: Arial, Helvetica, sans-serif"}
:::

::: {.title-bar .align-justify .hide-for-large .hc-header-background-color}
-   [![Документация
    Tantor](./pg_anon_files/TantorLogo.png "Документация Tantor")](https://wiki.astralinux.ru/tandocs)
-   [Документация
    Tantor](https://wiki.astralinux.ru/tandocs/dokumentatsiya-tantor-238752972.html){.header__navigation__menu-container--heading
    .hc-header-font-color}
:::

::: {#site-navigation .off-canvas .in-canvas-for-large .position-right .hc-header-background-color .is-transition-overlap .is-closed data-off-canvas="5842zu-off-canvas" data-transition="overlap" aria-hidden="false"}
::: {.top-bar .header__navigation__menu-container .hc-header-background-color .hc-header-font-color}
::: {.top-bar-left}
-   [![Документация
    Tantor](./pg_anon_files/TantorLogo.png "Документация Tantor")](https://wiki.astralinux.ru/tandocs)
-   [Документация
    Tantor](https://wiki.astralinux.ru/tandocs/dokumentatsiya-tantor-238752972.html){.header__navigation__menu-container--heading
    .hc-header-font-color}
:::

::: {.top-bar-right}
[×]{aria-hidden="true"}

-   [ Topics ]{.header__navigation__menu-container__menu--heading}
    ::: {#js-mobile-pageTree}
    -   -   -   -   -   
    :::

-   
:::
:::
:::

::: {role="main"}
[]{#admin-menu-link
style="display: none; position: absolute; top:-5000px;"}

::: {.grid-container .article}
::: {.grid-x .article__content data-sticky-container=""}
::: {#article-content .cell .medium-auto .grid-container .full}
-   [Документация
    Tantor](https://wiki.astralinux.ru/tandocs/dokumentatsiya-tantor-238752972.html)
-   [СУБД
    Tantor](https://wiki.astralinux.ru/tandocs/subd-tantor-238754116.html){.js-breadcrumbs-truncate}
-   [5. Дополнительно поставляемые
    модули](https://wiki.astralinux.ru/tandocs/5-dopolnitel-no-postavlyaemye-moduli-238754188.html){.js-breadcrumbs-truncate}
-   Инструмент поиска и маскирования конфиденциальных данных

Инструмент поиска и маскирования конфиденциальных данных {#инструмент-поиска-и-маскирования-конфиденциальных-данных .cell .article__heading role="heading"}
========================================================

::: {#content .section .cell .page role="main"}
::: {#main-content .cell .wiki-content .js-tocBot-content .hc-content-width--narrow}
::: {.sp-grid-section .conf-macro .output-block data-hasbody="true" data-macro-name="sp-pagelayout"}
::: {.sp-grid-cell .sp-grid-100}
Введение {#id-Инструментпоискаимаскированияконфиденциальныхданных-Введение}
========

\

Большинство ИТ компаний хранят и обрабатывают данные, представляющие
собой коммерческую тайну или данные содержащие персональную информацию
пользователей. Две этих группы данных можно обозначить, как
«сенситивные» данные. К персональным данным относятся: контактные
телефоны, паспортные данные и т.д. В Российской Федерации предусмотрен
закон от 27.07.2006 N 152-ФЗ "О персональных данных". В пункте 2 статьи
5 говорится, что обработка персональных данных должна ограничиваться
достижением конкретных, заранее определенных и законных целей. В статье
6 - обработка персональных данных осуществляется с согласия субъекта
персональных данных. Это требует от людей, чья работа связана с
информацией такого рода, особого внимания при разработке или
обслуживании программных комплексов, хранящих сенситивную информацию.

Наиболее частой задачей в процессе разработки становится перенос
содержимого БД из промышленного окружения в иные окружения с целью
нагрузочного тестирования или отладки функционала в процессе разработки.
К данным в контуре тестирования или разработки имеют доступ, как
правило, все сотрудники компании. Переносимая из промышленного окружения
БД не должна содержать сенситивные данные во избежание утечек.

Для решения рассматриваемой задачи был разработан инструмент pg\_anon,
позволяющий клонировать БД с заменой сенситивных данных на случайные или
хешированные значения. 

Терминология {#id-Инструментпоискаимаскированияконфиденциальныхданных-Терминология}
============

\

**Персональные (сенситивные) данные** - данные, в которых содержится
информация, которую недопустимо передавать в иные приемники хранения или
третьем лицам и которые являются коммерческой тайной.

**Словарь** - файл с расширением .py содержащий объект, в котором
описаны таблицы, поля и способы замены значений этих полей. Файл словаря
может быть написан вручную разработчиком или сгенерирован автоматически.

**Мета-словарь** - файл с расширением .py содержащий объект, в котором
описаны правила для поиска персональных (сенситивных) данных.
Мета-словарь создается пользователем вручную. Затем на основе
мета-словаря создается словарь. Процесс автоматического создания словаря
называется - разведка.

**Дамп** - процесс записи содержимого БД-источника в файлы, с
использованием указанного словаря. Дамп может быть частичным или полным.

**Рестор** - процесс загрузки данных из файлов в целевую БД. Целевая БД
не должна содержать объектов.

**Анонимизация (маскирование)** - процесс клонирования базы данных,
состоящий из этапов **дамп -\> рестор** в ходе которого сенситивные
данные будут заменены на случайные или хешированные значения.

**Функция анонимизации** - встроенная функция PostgreSQL или функция из
схемы anon\_funcs, меняющая входное значение на случайное или
хешированное. Схема anon\_funcs - содержит готовую библиотеку функций. В
эту схему могут быть добавлены новые функции для преобразования
анонимизируемых полей с последующим использованием в словарях.

Краткое описание pg\_anon {#id-Инструментпоискаимаскированияконфиденциальныхданных-Краткоеописаниеpg_anon}
=========================

\

pg\_anon - самостоятельная программа на языке Python, предназначенная
для работы с PostgreSQL (начиная с версии 9.6 и выше) и выполняющая
следующие задачи:

-   Создание схемы anon\_funcs, содержащей библиотеку функций для
    анонимизации
-   Поиск в БД PostgreSQL персональных (сенситивных) данных на основе
    мета-словаря
-   Создание словаря на основе результатов поиска (разведки)
-   Дамп и рестор с использованием словаря. Для разных баз данных могут
    быть предусмотрены отдельные файлы словарей
-   Синхронизация содержимого или структуры указанных таблиц между
    базой-источником и базой приемником

\

Визуальное представление терминов {#id-Инструментпоискаимаскированияконфиденциальныхданных-Визуальноепредставлениетерминов}
=================================

\

На диаграмме ниже изображен процесс переноса данных из базы-источника в
базу-приемник. В базе-источнике содержатся сенситивные данные, как
правило эта БД находится в промышленном окружении и к базе доступ имеет
строго фиксированное кол-во сотрудников.

![](./pg_anon_files/image2022-12-7_13-48-23.png){.confluence-embedded-image}

pg\_anon запускается доверенным администратором с указанием учетных
данных для подключения к базе-источнику и на основе указанного словаря
выполняется дамп в указанный каталог на файловой системе. В
рассматриваемом процессе используется заранее подготовленный и
согласованный с командой безопасности словарь. Далее полученный каталог
с файлами следует перенести на хост базы приемника. При переносе
каталога с дампом сжатие использовать нет необходимости, т.к. файлы
данных уже сжаты.

Когда каталог будет размещен на хосте с базой приемником следует
запустить процесс рестора с указанием учетных данных целевой базы.
Целевая база должна быть подготовлена заранее командой CREATE DATABASE и
не содержать никаких объектов. Если же в целевой БД окажутся
пользовательские таблицы, то процесс рестора не начнется. Когда рестор
завершится успешно, БД будет готова для задач разработки или
тестирования, в ходе которых к БД будет подключаться произвольное
количество сотрудников без риска утечки сенситивных данных.

Процесс создания словаря[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%9F%D1%80%D0%BE%D1%86%D0%B5%D1%81%D1%81%D1%81%D0%BE%D0%B7%D0%B4%D0%B0%D0%BD%D0%B8%D1%8F%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8F){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Процесссозданиясловаря}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Предусмотрены два процесса создания словаря:

-   Ручное создание словаря
-   Автоматическое создания словаря

На диаграмме ниже показаны оба процесса создания словаря.

![](./pg_anon_files/image2022-12-7_13-48-46.png){.confluence-embedded-image}

\

В случае с автоматическим созданием словаря используется мета-словарь и
pg\_anon запускается в режиме сканирования (разведки) базы-источника. В
ходе сканирования проверяются имена полей по заданным маскам, а также
содержимое полей по набору регулярных выражений и константных значений
(названия организаций, фамилии т.д.). Автоматически созданный словарь,
требует ревью команды информационной безопасности.

Ручное создание словаря предполагает, что команда разработки имеет
знания о структуре сопровождаемой БД и на основе эти знаний может
составить словарь, в котором будут перечислены все поля, содержащие
сенситивные данные. При ручном описании словаря возможны синтаксически
ошибки, которые можно проверить отдельными опциями для валидации словаря
(см. раздел «Валидация словаря полная и частичная»).

Как может быть улучшен поиск сенситивных данных (PoC) {#id-Инструментпоискаимаскированияконфиденциальныхданных-Какможетбытьулучшенпоисксенситивныхданных(PoC)}
=====================================================

\

Сканирование базы-источника в режиме разведки может быть слишком
ресурсоемким процессом. Чтобы оптимизировать процесс сканирования, поля,
содержащие сенситивные данные, заранее могут быть отмечены комментарием
с тэгом :sens. Расстановкой этого тэга должна заниматься команда
разработки и (или) команда информационной безопасности, как показано на
диаграмме ниже. 

![](./pg_anon_files/image2022-12-7_13-50-22.png){.confluence-embedded-image}

\

Тэг :sens может быть проставлен в репозитории проекта, или
непосредственно в самой БД. Важно учитывать, что если тэг был проставлен
в репозитории, то приложение для развертывания миграций должно доставить
команды COMMENT ON COLUMN до промышленного окружения.

Процесс расстановки тэгов в комментариях к полям может быть
автоматизирован на основе полученного в ходе сканирования БД словаря. То
есть автоматически сгенерированный словарь позволит проставить
комментарии ко всем полям, содержащим некоторое количество обычных или
сенситивных данных. 

![](./pg_anon_files/image2022-12-7_13-52-43.png){.confluence-embedded-image}

\

\

Если таблица была полностью просканирована на основе мета-словаря и
содержала более 1К строк, то поля этой таблицы могут быть автоматически
отмечены комментариями :sens и :no\_sens. Это позволит не сканировать
повторно всю БД, а только новые поля.

Если поля уже содержат комментарии, то к существующим комментариям будет
добавляться нужный тэг :sens или :no\_sens, в формате:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-sql .line-numbers tabindex="0" data-bidi-marker="true"}
COMMENT ON COLUMN users.addr
IS 'Это поле содержит адреса электронной почты :sens';
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
SQL
:::
:::

При повторном сканировании БД может быть указан словарь, полученный на
предыдущей итерации сканирования, это позволит пропустить значительное
количество ранее просканированных объектов, которые удовлетворяли
условию \"количество строк более 1К\".

Первичный запуск {#id-Инструментпоискаимаскированияконфиденциальныхданных-Первичныйзапуск}
================

\

Требования для запуска pg\_anon:

операционная система Linux

установленный Python версии 3.8 и выше

установленные модули из файла requirements.txt

1.  asyncpg - драйвер для работы с PostgreSQL.
2.  aioprocessing - предоставляет асинхронные и asyncio-совместимые
    версии 
3.  многих блокирующих методов объектов из библиотеки multiprocessing.
    Используется для распараллеливания работы по сканированию БД при
    использовании мета-словаря.
4.  nest-asyncio - предоставляет возможность создания вложенного цикла
    событий. Это требуется в много-процессном режиме для использования
    драйвера asyncpg

установленные клиентские приложения PostgreSQL (pg\_dump и pg\_restore
устанавливаются пакетным менеджером apt -y install postgresql-15
postgresql-client-15). Клиентские приложения должны по версии
соответствовать базе-источнику или быть новее\

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
cd pg_anon

python3 --version
>>
    Python 3.8.10 # python3 >= 3.8 OK

pip3 install -r requirements.txt
python3 pg_anon.py --version

>>

    2022-10-09 15:45:46,260     INFO 324 - Version 22.10.7
    # формат версии: год.месяц.день

ls log
>>
    init.log
    cat log/init.log 
>>

    2022-10-09 15:45:46,260     INFO 324 - Version 22.10.7
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

При каждом запуске pg\_anon логи записываются в каталог log с
добавлением в имя файла параметров запуска. Каждый файл лога не
превышает 10МБ. Это значение можно изменить в конструкторе класса
Context (файл pg\_anon.py).

Структура проекта и каталогов {#id-Инструментпоискаимаскированияконфиденциальныхданных-Структурапроектаикаталогов}
=============================

\

Весь проект состоит из 4х основных .py модулей, реализующих логику
соответствующих режимов работы. Ниже приведена структура каталога с
описанием файлов:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
├── common.py        # общие функции и классы
├── create_dict.py  # функционал сканирования БД на основе мета-словарей
├── dict            # каталог словарей и мета-словарей
│   ├── ...
├── docker          # файлы для сборки docker образа
├── dump.py         # функционал дампа БД
├── init.sql        # библиотека функций анонимизации
├── log         # каталог логов
│   └── init.log
├── output          # каталог для записи файлов БД при выполнении дампа
│   ├── ...
├── pg_anon.py      # главный файл (чтение параметров командной строки)
├── requirements.txt    # список модулей, от которых зависит pg_anon
├── restore.py      # функционал рестора
├── test          # каталог с файлами юнит тестов
│   ├── full_test.py    # общий сценарий тестирования
│   ├── init_env.sql    # SQL команды для инициализации тестового окружения
│   ├── __init__.py 
│   └── init_stress_env.sql # тестовое окружение для стресс тестирования (много объектов, много данных)
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\

Словари не обязательно располагать в каталоге dict, этот каталог
используется по умолчанию, если путь для словарей указан не полностью.

Перед началом работы {#id-Инструментпоискаимаскированияконфиденциальныхданных-Передначаломработы}
====================

\

Все режимы работы pg\_anon покрыты тестами, для обеспечения корректности
функционала в различных окружениях. Перед началом работы с новой версией
pg\_anon рекомендуется проверить работоспособность юнит-тестов (это
займет пару минут):

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
psql -c "CREATE USER anon_test_user WITH PASSWORD '123456' SUPERUSER" -U postgres

set TEST_DB_USER=anon_test_user
set TEST_DB_USER_PASSWORD=123456
set TEST_DB_HOST=127.0.0.1
set TEST_DB_PORT=5432
set TEST_SOURCE_DB=test_source_db
set TEST_TARGET_DB=test_target_db
su - postgres       # запуск тестов от имени postgres
python3 test/full_test.py -v
>>
    Ran N tests in ...
    OK
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Учетные данные для подключения к тестовым базам данных указываются через
переменные окружения с префиксом TEST\_, как показано во фрагменте выше.
Сценарий тестирования предполагает, что базы «источник» и «приемник»
находятся на одном инстансе и pg\_anon запускается на том же хосте.

Успешное прохождение всех тестов гарантирует работоспособность pg\_anon
в используемом окружении (Python, PostgreSQL, ОС). Если возникла ошибка:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
asyncpg.exceptions.ExternalRoutineError: program "gzip > .../test/...abcd.dat.gz" failed
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\

это означает, что у пользователя postgres недостаточно полномочий для
доступа к каталогу, в который происходит запись программой gzip. То есть
pg\_anon был запущен от некоторого пользователя, затем был создан
выходной каталог с этим пользователем в качестве владельца, затем
PostgreSQL попытался от имени postgres записать файл в этот каталог. Как
может быть исправлена ошибка:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
usermod -a -G current_user_name postgres
chmod -R g+rw /home/current_user_name/Desktop/pg_anon
chmod g+x /home/current_user_name/Desktop/pg_anon/output/test
# валидация полномочий
su - postgres
touch /home/current_user_name/Desktop/pg_anon/test/1.txt
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Параметры и режимы работы pg\_anon {#id-Инструментпоискаимаскированияконфиденциальныхданных-Параметрыирежимыработыpg_anon}
==================================

\

Общие параметры **для всех режимов работы**:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
--debug    # режим отладки (по умолчанию false)
--verbose = [debug, info, warning, error]   # уровень информативности (по умолчанию info). Все сообщения дублируются в stdout и в файл логов. Уровень "debug" требует указания одноименной опции --debug
--threads  # количество потоков следует указывать не более кол-ва доступных ядер CPU, по умолчанию 4 потока
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

В режиме отладки в логи записывается избыточное количество информации,
этот режим рекомендуется только для диагностики неполадок или отправки
отчетов об ошибках. Логи не содержат сенситивные данных, но содержат IP
адреса подключений и имена обрабатываемых объектов: схем, таблиц и
полей.

Режимы работы определяются параметром \--mode:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
--mode=init              # инициализировать библиотеку функций для анонимизации в базе-источнике
--mode=create-dict          # сканировать БД с использованием мета-словаря
--mode=dump                 # выполнить дамп БД с использованием словаря
--mode=restore              # выполнить загрузку анонимизированных данных в БД-приемник
--mode=sync-data-dump       # выполнить дамп данных только указанных таблиц
--mode=sync-data-restore    # выполнить рестор только данных
--mode=sync-struct-dump     # выполнить дамп структуры БД только указанных таблиц
--mode=sync-struct-restore  # выполнить восстановление структуры БД
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Параметры подключения к БД (для каждого режима работы):

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
--db-host            # хост для подключения к БД (источнику или приемнику)
--db-port               # порт
--db-name               # имя БД источника или приемника
--db-user               # пользователь с правами SUPERUSER (требуется для выполнения команд COPY). Команда COPY разрешена только суперпользователям базы данных или пользователям, которым предоставлена одна из ролей: pg_read_server_files,           pg_write_server_files или pg_execute_server_program, поскольку это позволяет читать или записывать любой файл или запускать программу, к которой у сервера есть права доступа
--db-user-password      # пароль для подключения к БД (источнику или приемнику). Пароль так же может быть передан через стандартную переменную PGPASSWORD. Утилиты pg_dump и pg_restore, запускаемые программой pg_anon, принимают пароль в любом случае через переменную PGPASSWORD
--db-passfile           # путь к .pgpass файлу, содержащему пароли для подключения
--db-ssl-key-file       # закрытый ключ сервера для зашифрованного подключения. Все файлы ключей для защищенного подключения должны иметь корректные полномочия - chmod 600
--db-ssl-cert-file      # сертификат сервера для зашифрованного подключения
--db-ssl-ca-file        # файл сертификата удостоверяющего центра. Позволяет проверить, что сертификат клиента подписан доверенным центром сертификации
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Более подробно о параметрах подключения к БД см. документацию драйвера
[asyncpg](https://magicstack.github.io/asyncpg/current/api/index.html){.external-link}.

Размещение pg\_anon: локально с БД {#id-Инструментпоискаимаскированияконфиденциальныхданных-Размещениеpg_anon:локальносБД}
==================================

\

Наиболее удобным для эксплуатации способом размещения pg\_anon является
приведенный на диаграмме способ:

![](./pg_anon_files/image2022-12-7_22-18-12.png){.confluence-embedded-image}

pg\_anon при запуске в режиме дампа записывает данные в каталог output.
В этот же каталог в режиме дампа СУБД командой COPY TO записывает
\*.dat.gz файлы от имени postgres. Имя каталога назначается на основе
имени словаря, т.е. если для дампа был использован словарь
some\_dict.py, то данные будут записаны в каталог с именем some\_dict. В
каталог записываются данные двумя процессами: pg\_anon и СУБД.

Размещение pg\_anon: удалённо {#id-Инструментпоискаимаскированияконфиденциальныхданных-Размещениеpg_anon:удалённо}
=============================

\

В режиме сканирования запуск pg\_anon рекомендуется делать с отдельного
хоста, т.к. сканирование выполняется в несколько процессов и может
утилизировать все доступные ядра процессора, что повлияет на
производительность сервера БД. Результирующий словарь будет записан в
локальный каталог dict.

Если pg\_anon запущен для дампа на отдельном хосте, то происходит сплит
каталога при создании дампа, как показано на диаграмме:

![](./pg_anon_files/image2022-12-7_22-18-40.png){.confluence-embedded-image}

То есть СУБД пишет данные в локальный каталог, а pg\_anon в каталог,
который на том же отдельном хосте. Поэтому на обоих хостах должен
существовать одинаковый каталог.

Это поведение является особенностью текущей реализации и может быть
изменено с использованием метода
[copy\_from\_query](https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.pool.Pool.copy_from_query){.external-link}.
Это позволит записывать файлы данных на том же хосте, где запущен
pg\_anon.

Инициализация библиотеки функций для анонимизации {#id-Инструментпоискаимаскированияконфиденциальныхданных-Инициализациябиблиотекифункцийдляанонимизации}
=================================================

\

Прежде чем выполнить дамп БД в первый раз, следует инициализировать
библиотеку функций для анонимизации опцией \--mode=init. На диаграмме
ниже показан пример запуска pg\_anon в режиме инициализации. Пароль
может быть задан отдельно с помощью переменной окружения PGPASSWORD.

![](./pg_anon_files/image2022-12-7_22-19-10.png){.confluence-embedded-image}

При инициализации в базе-источнике будет создана схема anon\_funcs
содержащую функции. Эти функции позволяют решать наиболее
распространенные задачи по маскированию данных и будут рассмотрены в
следующем разделе.

Описание библиотеки функций для анонимизации {#id-Инструментпоискаимаскированияконфиденциальныхданных-Описаниебиблиотекифункцийдляанонимизации}
============================================

\

Список доступных для использования в словарях функций:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-sql .line-numbers tabindex="0" data-bidi-marker="true"}
# добавить шум к вещественному числу

anon_funcs.noise(100, 1.2) -> 123

anon_funcs.dnoise('2020-02-02 10:10:10'::timestamp, interval '1 month')
-> '2020-03-02 10:10:10'
anon_funcs.digest('text', 'salt', 'sha256') -> '3353e....'
anon_funcs.partial('123456789',1,'***',3) -> '1***789'
anon_funcs.partial_email('example@gmail.com') -> 'ex******@gm******.com'
anon_funcs.random_string(7) -> 'H3ZVL5P'
anon_funcs.random_zip() -> 851467
anon_funcs.random_date_between('2020-02-02 10:10:10'::timestamp, '2022-02-05 10:10:10'::timestamp) -> '2021-11-08 06:47:48.057'
anon_funcs.random_date() -> '1911-04-18 21:54:13.139'
anon_funcs.random_int_between(100, 200) -> 159
anon_funcs.random_bigint_between(6000000000, 7000000000) -> 6268278565
anon_funcs.random_phone('+7') -> +7297479867
anon_funcs.random_hash('seed', 'sha512') -> 'a06b3039...'
anon_funcs.random_in(array['a', 'b', 'c']) -> 'a'
anon_funcs.hex_to_int('8AB') -> 2219
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
SQL
:::
:::

\

В дополнение к существующим функциям схемы anon\_funcs могут быть
использованы функции из расширения
[pgcrypto](https://www.postgresql.org/docs/current/pgcrypto.html){.external-link}.
Пример использования шифрования с преобразованием в base64 для хранение
шифрованного значения в поле типа text:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-sql .line-numbers tabindex="0" data-bidi-marker="true"}
CREATE EXTENSION IF NOT EXISTS pgcrypto;

select encode((select encrypt('data', 'password', 'bf')), 'base64') 
>>
    cSMq9gb1vOw=

select decrypt(
(
select decode('cSMq9gb1vOw=', 'base64')
), 'password', 'bf')
>>
    data
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
SQL
:::
:::

Режим работы create-dict (разведка) {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыcreate-dict(разведка)}
===================================

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-sql .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \

    ... # параметры для подключения к БД
    --scan-mode=full # [partial, full] полное или частичное сканирование
    --dict-file=meta_dict.py # файл мета-словаря
    --output-dict-file=out_dict.py # результирующий словарь будет сохранен в каталог dict/out_dict.py
    --scan-partial-rows=10000 # кол-во строк для обработки в рамках одной операции
    --mode=create-dict
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
SQL
:::
:::

![](./pg_anon_files/image2022-12-7_22-22-58.png){.confluence-embedded-image}

Режим работы create-dict: формат мета-словаря {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыcreate-dict:форматмета-словаря}
=============================================

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-js .line-numbers tabindex="0" data-bidi-marker="true"}
{

   "field": {   # какие поля анонимизировать?
      "rules": [...],      # список регулярных выражений для поиска полей по имени
      "constants": [...]   # список константных имен полей
   },

   "skip_rules": [   # список схем, таблиц и полей, которые следует проигнорировать
      {...}   # возможно в какой-то схеме или таблице содержится много данных, которые нет смысла сканировать
   ],

   "data_regex": {
      "rules": [...]
   },

   "data_const": {
      "constants": [...]
   },
   "funcs": {...} # список типов полей (int, text, ...) и функции для анонимизации
}
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
JS
:::
:::

\

Клонирование БД {#id-Инструментпоискаимаскированияконфиденциальныхданных-КлонированиеБД}
===============

Режим работы dump[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bdump){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыdump}
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ... # параметры для подключения к БД
    --dict-file=some_dict.py # словарь для анонимизации
    --mode=dump
    --clear-output-dir # очистить выходной каталог output/some_dict
    --pg_dump=/usr/bin/pg_dump   # значение по умолчанию
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

![](./pg_anon_files/image2022-12-7_22-24-53.png){.confluence-embedded-image}

Режим работы dump: формат словаря[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bdump:%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%82%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8F){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыdump:форматсловаря}
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-js .line-numbers tabindex="0" data-bidi-marker="true"}
{
    "dictionary": [     # список объектов для анонимизации
        {
            "schema":"schm_other_1", "table":"some_tbl", # конкретная таблица
            "fields": {"val":"'text const'"}  # объект "поля" содержит вызов ф-ций или SQL
        },
        {
            "schema_mask": "^schm_mask_incl", "table_mask": "^some_t", # объекты по маске
            # допустимо указать символ *
            "fields": {"val": "md5(val)"}
        }
    ],
    "dictionary_exclude": [  # список объектов которые будут перенесены "как есть"
        { "schema":"schm_other_2", "table":"exclude_tbl" }
        # { "schema":"*", "table":"*" } - не переносить содержимое всех таблиц, кроме указанных
    ]
}
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
JS
:::
:::

\

Режим работы dump: структура выходного каталога[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bdump:%D1%81%D1%82%D1%80%D1%83%D0%BA%D1%82%D1%83%D1%80%D0%B0%D0%B2%D1%8B%D1%85%D0%BE%D0%B4%D0%BD%D0%BE%D0%B3%D0%BE%D0%BA%D0%B0%D1%82%D0%B0%D0%BB%D0%BE%D0%B3%D0%B0){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыdump:структуравыходногокаталога}
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...
    --mode=dump
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\

Формат файла metadata.json[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A4%D0%BE%D1%80%D0%BC%D0%B0%D1%82%D1%84%D0%B0%D0%B9%D0%BB%D0%B0metadata.json){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Форматфайлаmetadata.json}
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

\

При создании дампа записывается metadata.json файл, описывающий
содержимое каталога и используемый в процессе рестора для проверки
версии базы-приемника, версии утилиты pg\_restore и свободного дискового
пространства:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-js .line-numbers tabindex="0" data-bidi-marker="true"}
{
    "db_size": 44581667,    # размер базы-источника
    "created": "23/10/2022 15:48:17",  # дата создания дампа
    "seq_lastvals": {       # состояния последовательностей для всех таблиц у которых они были на момент дампа
        "public.tbl_100_id_seq": {
            "schema": "public",
            "seq_name": "tbl_100_id_seq",
            "value": 15120
        }
    },

    "pg_version": "14.5",       # версия базы-источника
    "pg_dump_version": "14.5",  # версия pg_dump-а, использованного при создании дампа
    "dictionary_content_hash": "01f6f080...d10f6", # хэш словаря, использованного при дампе
    "dict_file": "test.py",  # имя словаря, использованного при дампе
    "files": {   # объект с набором файлов, где имена атрибутов это названия файлов, на одну таблицу один файл 
        "0f1d55351607b9403a0350877da95c52.dat.gz": {
            "schema": "public",   # описание файла: схема, таблица, кол-во строк
            "table": "tbl_100",
            "rows": "15120"
        }
    },

    "total_tables_size": 34029568,  # общий размер таблицы, используемый для оценки свободного пространства перед выполнением рестора
    "total_rows": 317544    # кол-во строк всех таблиц
}
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
JS
:::
:::

Валидация словаря полная и частичная[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%92%D0%B0%D0%BB%D0%B8%D0%B4%D0%B0%D1%86%D0%B8%D1%8F%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8F%D0%BF%D0%BE%D0%BB%D0%BD%D0%B0%D1%8F%D0%B8%D1%87%D0%B0%D1%81%D1%82%D0%B8%D1%87%D0%BD%D0%B0%D1%8F){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Валидациясловаряполнаяичастичная}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...          # параметры для подключения к БД
    --mode=dump
    --validate-dict # проверить словарь выполнив SQL запросы с LIMIT-ом без экспорта данных (проверяется структура словаря и валидность SQL запросов)
    --validate-full # то же что validate-dict + экспорт в файл
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

[](https://wiki.astralinux.ru/tandocs/files/238755875/238756018/1/1670445003029/image2022-12-7_22-30-2.png){.js-enlargeOnClick
.cboxElement}

[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-image2022-12-7_22-30-2.png){.js-anchor
.article__content__content__anchor}

\

Режим работы restore[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Brestore){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыrestore}
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...              # параметры для подключения к БД
    --input-dir=some_dict   # каталог с файлами БД (либо указать полный путь)
    --mode=restore
    --pg_restore=/usr/bin/pg_restore   # значение по умолчанию
    --disable-checks        # отключить различные проверки перед загрузкой в приемник
    --seq-init-by-max-value # инициализировать сиквенсы на основе максимальных зн-ий
    --drop-custom-check-constr   # удалить пользовательские check констрейнты
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

![](./pg_anon_files/image2022-12-7_22-31-29.png){.confluence-embedded-image}

Частичная синхронизация данных {#id-Инструментпоискаимаскированияконфиденциальныхданных-Частичнаясинхронизацияданных}
==============================

Режим работы sync-data-dump[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-data-dump){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-data-dump}
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

\

**Задача:** синхронизировать содержимое набора таблиц

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...              # параметры для подключения к БД
    --dict-file=some_dict.py
    --mode=sync-data-dump
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\
\

![](./pg_anon_files/image2022-12-7_22-32-14.png){.confluence-embedded-image}

Режим работы sync-data-dump: формат словаря[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-data-dump:%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%82%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8F){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-data-dump:форматсловаря}
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

\

В словаре указываются **таблицы и поля** для синхронизации структуры:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-js .line-numbers tabindex="0" data-bidi-marker="true"}
{
    "dictionary": [
        {
            "schema":"schm_other_2",
            "table":"exclude_tbl",
            "fields": {"val":"'text const modified'"}
        },
        {
            "schema":"schm_other_2",
            "table":"some_tbl",
            "raw_sql": "SELECT id, val || ' modified 2' as val FROM schm_other_2.some_tbl"
        }
    ],
    "dictionary_exclude": [{"schema_mask": "*", "table_mask": "*"}]
}
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
JS
:::
:::

\

Режим работы sync-data-restore[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-data-restore){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-data-restore}
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Параметры запуска:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...              # параметры для подключения к БД
    --input-dir=some_dict   # каталог с файлами БД (либо указать полный путь)
    --mode=restore
    --disable-checks        # отключить различные проверки перед загрузкой в
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\
\

![](./pg_anon_files/image2022-12-7_22-34-7.png){.confluence-embedded-image}

Синхронизация структуры БД {#id-Инструментпоискаимаскированияконфиденциальныхданных-СинхронизацияструктурыБД}
==========================

Режим работы sync-struct-dump[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-struct-dump){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-struct-dump}
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

**Задача:** синхронизировать структуру указанного набора таблиц

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...              # параметры для подключения к БД
    --dict-file=some_dict.py    # словарь для переноса структуры БД
    --mode=sync-struct-dump
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

[](https://wiki.astralinux.ru/tandocs/files/238755875/238756022/1/1670445311942/image2022-12-7_22-35-11.png){.js-enlargeOnClick
.cboxElement}

[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-image2022-12-7_22-35-11.png){.js-anchor
.article__content__content__anchor}

Режим работы sync-struct-dump: формат словаря[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-struct-dump:%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%82%D1%81%D0%BB%D0%BE%D0%B2%D0%B0%D1%80%D1%8F){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-struct-dump:форматсловаря}
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

\

В словаре указываются **только таблицы** для синхронизации структуры:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-js .line-numbers tabindex="0" data-bidi-marker="true"}
{
    "dictionary": [
        {
            "schema":"schm_other_2",
            "table":"exclude_tbl"
        },
        {
            "schema":"schm_other_2",
            "table":"some_tbl"
        }
    ],
    "dictionary_exclude": [{"schema_mask": "*", "table_mask": "*"}]
}
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
JS
:::
:::

\

Режим работы sync-struct-restore[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A0%D0%B5%D0%B6%D0%B8%D0%BC%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8Bsync-struct-restore){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Режимработыsync-struct-restore}
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Параметры запуска:

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
python3 pg_anon.py \
    ...              # параметры для подключения к БД
    --input-dir=some_dict   # каталог с частичной структурой БД (либо указать полный путь)
    --mode=sync-struct-restore
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

[](https://wiki.astralinux.ru/tandocs/files/238755875/238756023/1/1670445428348/image2022-12-7_22-37-7.png){.js-enlargeOnClick
.cboxElement}

Сценарии {#id-Инструментпоискаимаскированияконфиденциальныхданных-Сценарии}
========

\

::: {.table-wrap}
+-----------------------------------+-----------------------------------+
| **Описание**                      | **Набор действий**                |
+-----------------------------------+-----------------------------------+
| Клонировать БД целиком (структура | \--mode=init                      |
| + данных всех таблиц)             | *(только при первом подключении к |
|                                   | БД-источнику)*                    |
|                                   |                                   |
|                                   | \--mode=create-dict               |
|                                   | *(опционально, если требуется     |
|                                   | сгенерировать словарь)*           |
|                                   |                                   |
|                                   | \--mode=dump                      |
|                                   |                                   |
|                                   | \--mode=restore                   |
+-----------------------------------+-----------------------------------+
| Клонировать часть БД              | \--mode=init                      |
|                                   | *(только при первом подключении к |
| (**структура будет перенесена     | БД-источнику)*                    |
| полностью**, данные будут         |                                   |
| перенесены только для указанных   | \--mode=dump                      |
| таблиц)                           | *(указать словарь с секцией       |
|                                   | dictionary\_exclude \* \*. в      |
|                                   | dictionary указать некоторые      |
|                                   | таблицы)*                         |
|                                   |                                   |
|                                   | \--mode=restore                   |
+-----------------------------------+-----------------------------------+
| Клонировать часть БД:             | \--mode=init                      |
| определенный набор таблиц (только | *(только при первом подключении к |
| структура)                        | БД-источнику)*                    |
|                                   |                                   |
|                                   | \--mode=sync-struct-dump          |
|                                   |                                   |
|                                   | \--mode=sync-struct-restore       |
+-----------------------------------+-----------------------------------+
| Синхронизировать данные           | \--mode=init                      |
| определенного списка таблиц       | *(только при первом подключении к |
|                                   | БД-источнику)*                    |
|                                   |                                   |
|                                   | \--mode=sync-data-dump            |
|                                   |                                   |
|                                   | \--mode=sync-data-restore         |
+-----------------------------------+-----------------------------------+
:::

\

Docker образ {#id-Инструментпоискаимаскированияконфиденциальныхданных-Dockerобраз}
============

\

Поставка pg\_anon может выполнятся с использованием docker-а. Это
позволит не устанавливать нужную версию Python и используемые модули.

Сборка[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%A1%D0%B1%D0%BE%D1%80%D0%BA%D0%B0){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Сборка}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
cd pg_anon/docker
make PG_VERSION=15
docker tag $(docker images -q | head -n 1) pg_anon:pg15
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Перенос образа[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%9F%D0%B5%D1%80%D0%B5%D0%BD%D0%BE%D1%81%D0%BE%D0%B1%D1%80%D0%B0%D0%B7%D0%B0){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Перенособраза}
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
docker tag $(docker images -q | head -n 1) pg_anon:pg15
docker save -o pg_anon_22_9_12.tar pg_anon:pg15
docker load < pg_anon_22_9_12.tar    # на целевом хосте загрузить образ из архива
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

Запуск[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%97%D0%B0%D0%BF%D1%83%D1%81%D0%BA){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Запуск}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
# Если при запуске возникает ошибка "The container name "/pg_anon" is already in use"
# docker rm -f pg_anon

docker run --name pg_anon -d pg_anon:pg14
docker exec -it pg_anon bash
python3 test/full_test.py -v
exit

# Run and mount directory from HOST to /usr/share/pg_anon_from_host
docker rm -f pg_anon
docker run --name pg_anon -v $PWD:/usr/share/pg_anon -d pg_anon:pg14
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\

Отладка контейнера[](https://wiki.astralinux.ru/tandocs/instrument-poiska-i-maskirovaniya-konfidentsial-nyh-dannyh-238755875.html#id-%D0%98%D0%BD%D1%81%D1%82%D1%80%D1%83%D0%BC%D0%B5%D0%BD%D1%82%D0%BF%D0%BE%D0%B8%D1%81%D0%BA%D0%B0%D0%B8%D0%BC%D0%B0%D1%81%D0%BA%D0%B8%D1%80%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D1%8F%D0%BA%D0%BE%D0%BD%D1%84%D0%B8%D0%B4%D0%B5%D0%BD%D1%86%D0%B8%D0%B0%D0%BB%D1%8C%D0%BD%D1%8B%D1%85%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85-%D0%9E%D1%82%D0%BB%D0%B0%D0%B4%D0%BA%D0%B0%D0%BA%D0%BE%D0%BD%D1%82%D0%B5%D0%B9%D0%BD%D0%B5%D1%80%D0%B0){.js-anchor .article__content__content__anchor} {#id-Инструментпоискаимаскированияконфиденциальныхданных-Отладкаконтейнера}
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

::: {.code-macro .conf-macro .output-block data-hasbody="true" data-macro-name="sp-macrooverride-plaintextbody-block"}
::: {.code-toolbar}
``` {.language-bash .line-numbers tabindex="0" data-bidi-marker="true"}
docker exec -it pg_anon bash
>>
    Error response from daemon: Container c876d... is not running

docker logs c876d...

# Fix errors in entrypoint.sh
# Set "ENTRYPOINT exec /entrypoint_dbg.sh" in Dockerfile

docker rm -f pg_anon
make PG_VERSION=14
docker tag $(docker images -q | head -n 1) pg_anon:pg14
docker run --name pg_anon -d pg_anon:pg14
docker exec -it pg_anon bash
```

::: {.toolbar}
::: {.toolbar-item}
Copy
:::
:::
:::

::: {.code-macro__language}
BASH
:::
:::

\
\
\
:::
:::
:::
:::
:::
:::
:::
:::

::: {.grid-x .footer--alignment .hc-footer-font-color}
::: {.cell .large-shrink .hc-footer-font-color}
![Документация Tantor
Logo](./pg_anon_files/footer-logo.svg "Документация Tantor"){.footer__logo}
:::

::: {.grid-x .cell .large-auto}
::: {.cell .footer__links}
:::

::: {.cell}
[ Copyright © 2022 Tantor Labs • Powered by [Scroll
Viewport](https://www.k15t.com/go/scroll-viewport-help-center) and
[Atlassian Confluence](https://www.atlassian.com/software/confluence)
]{.footer__attribution-line--copyright .hc-footer-font-color}
:::
:::
:::

::: {#tableOverlay .table-overlay .full .reveal .without-overlay data-reveal="v74qjy-reveal" role="dialog" aria-hidden="true" data-yeti-box="tableOverlay" data-resize="tableOverlay" data-events="resize" style="top: 0px; left: 0px; margin: 0px;"}
[×]{aria-hidden="true"}

::: {.article__content .table-overlay__content}
:::
:::

::: {.sp-blanket style="background: #000; height: 100%; left: 0px; opacity: 0.5; position: fixed; top: 0; width: 100%; z-index: 2500; display: none"}
:::

::: {#cboxOverlay style="display: none;"}
:::

::: {#colorbox role="dialog" tabindex="-1" style="display: none;"}
::: {#cboxWrapper}
<div>

::: {#cboxTopLeft style="float: left;"}
:::

::: {#cboxTopCenter style="float: left;"}
:::

::: {#cboxTopRight style="float: left;"}
:::

</div>

::: {style="clear: left;"}
::: {#cboxMiddleLeft style="float: left;"}
:::

::: {#cboxContent style="float: left;"}
::: {#cboxTitle style="float: left;"}
:::

::: {#cboxCurrent style="float: left;"}
:::

::: {#cboxLoadingOverlay style="float: left;"}
:::

::: {#cboxLoadingGraphic style="float: left;"}
:::
:::

::: {#cboxMiddleRight style="float: left;"}
:::
:::

::: {style="clear: left;"}
::: {#cboxBottomLeft style="float: left;"}
:::

::: {#cboxBottomCenter style="float: left;"}
:::

::: {#cboxBottomRight style="float: left;"}
:::
:::
:::

::: {style="position: absolute; width: 9999px; visibility: hidden; display: none; max-width: none;"}
:::
:::
