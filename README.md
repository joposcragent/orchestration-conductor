# orchestration-conductor

Сервис оркестрации асинхронных джобов: REST enqueue в Kafka, потребление
`collection-batch` и `job-posting-create`, вызов settings-manager (Feign) и
публикация следующих шагов в Kafka.

## Требования

- JDK 21
- Docker (для локальной БД и Flyway при генерации jOOQ)

## Схема БД и jOOQ

Кодогенерация jOOQ читает схему `orchestration` в PostgreSQL. Сервис в рантайме
к БД не подключается; сгенерированные классы — заготовка под будущее.

1. Поднимите Postgres и примените миграции. Удобный вариант — compose из
   репозитория спецификаций
   ([`docker-compose.yaml` в database-schema][db-compose]).

   В том compose для образа Flyway используется переменная `FLYWAY_IMAGE_TAG`
   (по умолчанию `latest`). При ручном запуске образа можно задать тег так:

   ```bash
   docker pull "joposcragent/flyway:${FLYWAY_TAG:-latest}"
   ```

2. Задайте параметры подключения для Gradle и выполните генерацию:

   | Переменная | Назначение |
   | ---------- | ---------- |
   | `JOOQ_DB_URL` | JDBC URL (см. ниже) |
   | `JOOQ_DB_USER` | пользователь БД (`postgres`) |
   | `JOOQ_DB_PASSWORD` | пароль (`postgres`) |

   Значения по умолчанию для URL:
   `jdbc:postgresql://localhost:5432/joposcragent`.

   ```bash
   export JOOQ_DB_URL="jdbc:postgresql://localhost:5432/joposcragent"
   export JOOQ_DB_USER="postgres"
   export JOOQ_DB_PASSWORD="postgres"
   ./gradlew generateJooq
   ```

3. Сборка компилирует Kotlin после задач генерации OpenAPI и jOOQ:

   ```bash
   ./gradlew check
   ```

Порог покрытия строк (JaCoCo) для рукописного кода включён в `check`
(не ниже 60%).

## Конфигурация

Основные свойства см. `src/main/resources/application.yaml`: порт по умолчанию
**8084**, Kafka bootstrap, группы consumer, базовый URL settings-manager
(`joposcragent.settings-manager.base-url`).

## Образ приложения

```bash
export IMAGE_NAME="joposcragent/orchestration-conductor"
export IMAGE_TAG="1.0.0"
./gradlew bootBuildImage
```

После успешной сборки задача `bootBuildImageTagLatest` помечает образ тегом
`latest` (см. `build.gradle.kts`).

## Спецификация

Поведение REST и Kafka описано в каталоге
[orchestration-conductor][spec-dir].

[db-compose]: ../../specifications/database-schema/docker-compose.yaml
[spec-dir]: ../../specifications/services/orchestration-conductor/index.md
