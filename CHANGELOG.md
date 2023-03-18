1.0.4 (2023-03-19)
==================

- [fix] [#32](https://github.com/civitaspo/embulk-input-dynamodb/pull/32) Remove unnecessary if clause (follow up [#31](https://github.com/civitaspo/embulk-input-dynamodb/pull/31)).

1.0.3 (2023-02-17)
==================

- [feat] [#31](https://github.com/civitaspo/embulk-input-dynamodb/pull/31) Add `@tailrec` annotation to `runInternal` in operations.

1.0.2 (2023-02-15)
==================

- [chore] [#30](https://github.com/civitaspo/embulk-input-dynamodb/pull/30) Fix scan log message.

1.0.1 (2023-02-15)
==================

- [chore] [#29](https://github.com/civitaspo/embulk-input-dynamodb/pull/29) Upgrade dependencies.
    - Use `Exec.getPageBuilder` instead of `new PageBuilder`.
    - Upgrade scala 2.13.1 -> 2.13.10
    - Upgrade aws-sdk 1.11.171 -> 1.12.406

1.0.0 (2023-02-15)
==================

- [Breaking Change] [#27](https://github.com/civitaspo/embulk-input-dynamodb/pull/27) Upgrade embulk 0.9.23 -> 0.10.41 with removing deprecated features.
    - Upgrade Gradle 6.1 -> 7.6
    - Apply gradle-embulk-plugins
    - Remove deprecated features
    - Upgrade embulk 0.9.23 -> 0.10.41

0.3.1 (2020-03-29)
==================

- [BugFix] [#23](https://github.com/civitaspo/embulk-input-dynamodb/pull/23) Throw scala.MatchError when using "limit" option. (Fix [#21](https://github.com/civitaspo/embulk-input-dynamodb/issues/21))
- [Enhancement] [#23](https://github.com/civitaspo/embulk-input-dynamodb/pull/23) Use embulk-core tests library instead of embulk-test.

0.3.0 (2020-03-09)
==================

- [Enhancement] Update dependencies
    - [#5](https://github.com/civitaspo/embulk-input-dynamodb/pull/5) JRuby Gradle Plugin (0.1.5 => 1.5.0)
    - [#6](https://github.com/civitaspo/embulk-input-dynamodb/pull/6) Scala (2.11.8 => 2.13.1)
    - [#7](https://github.com/civitaspo/embulk-input-dynamodb/pull/7) AWS DynamoDB SDK (1.10.43 => 1.11.711)
    - [#8](https://github.com/civitaspo/embulk-input-dynamodb/pull/8) Embulk (0.8.13 => 0.9.23)
- [Enhancement] [#9](https://github.com/civitaspo/embulk-input-dynamodb/pull/9) Use TestingEmbulk instead of EmbulkEmbed when testing
- [Enhancement] [#10](https://github.com/civitaspo/embulk-input-dynamodb/pull/10) Reduce test dependencies
- [Enhancement] [#13](https://github.com/civitaspo/embulk-input-dynamodb/pull/13) Use Github Actions instead of CircleCI.
- [Enhancement] [#15](https://github.com/civitaspo/embulk-input-dynamodb/pull/15) Improve development environments
    - Introduce [scalafmt](https://scalameta.org/scalafmt/) with [spotless](https://github.com/diffplug/spotless)
    - Fix the format violations
    - Add [scalafmt](https://scalameta.org/scalafmt/) to CI
    - Add [CHANGELOG](./CHANGELOG.md)
    - Add [an example](./example)
    - Update README about development
- [Enhancement] [#16](https://github.com/civitaspo/embulk-input-dynamodb/pull/16) Cleanup gradle settings
- [New Feature] [#18](https://github.com/civitaspo/embulk-input-dynamodb/pull/18) Introduce new `auth_method`: `"session"`, `"anonymous"`, `"web_identity_token"`, `"default"`.
    - `"anonymous"`: uses anonymous access. This auth method can access only public files.
    - `"session"`: uses temporary-generated **access_key_id**, **secret_access_key** and **session_token**.
    - `"assume_role"`: uses temporary-generated credentials by assuming **role_arn** role.
    - `"web_identity_token"`: uses temporary-generated credentials by assuming **role_arn** role with web identity.
    - `"default"`: uses AWS SDK's default strategy to look up available credentials from runtime environment. This method   behaves like the combination of the following methods.
        1. `"env"`
        1. `"properties"`
        1. `"web_identity_token"`
        1. `"profile"`
        1. `"instance"`
- [New Feature] [#18](https://github.com/civitaspo/embulk-input-dynamodb/pull/18) Support `http_proxy` option when generating aws credentials.
- [Enhancement] [#18](https://github.com/civitaspo/embulk-input-dynamodb/pull/18) The default value of `auth_method` option become `"default"`. When `access_key_id` and `secret_access_key` options are set, use `"basic"` as `auth_method` for backward compatibility.
- [Deprecated] [#18](https://github.com/civitaspo/embulk-input-dynamodb/pull/18) Make `access_key` and `secret_key` options deprecated. Use `access_key_id` and `secret_access_key` options instead.
- [Deprecated] [#18](https://github.com/civitaspo/embulk-input-dynamodb/pull/18) Make `end_point` option deprecated. Use `endpoint` option instead.
- [Deprecated] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) The original operation implementation is deprecated, so the below options become deprecated.
    - **operation**: Use **query** option or **scan** option instead.
    - **limit**: Use **query.batch_size** option or **query.batch_size** instead.
    - **scan_limit**: Use **query.batch_size** option or **query.batch_size** instead.
    - **record_limit**: Use **query.limit** option or **query.limit** instead.
    - **filters**: Use **query.filter_expression** option or **query.filter_expression** instead.
- [New Feature] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) Introduce new options **scan**, **query** to support all configurations for Dynamodb Scan/Query Operation API except legacy configurations.
    - NOTE: This operation stores `null` AttributeValue as `null`, though, in the deprecated operation, `null` is converted arbitrarily. (`string` -> empty string, `long` -> `0`, `double` -> `0.0`, `boolean` -> `false`)
    - NOTE: This operation stores timestamp values by parsing user-defined format, though the deprecated operation skips storing values when the column type is defined as `timestamp` without any errors.
    - NOTE: This operation can convert the specific type of the attribute that you specify in **column.attribute_type** to Embulk types, though the deprecated operation can only convert Embulk types that match a particular Dynamodb Attribute type.
- [Enhancement] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) You can store each dynamodb item as JSON, so **columns** option becomes optional.
- [Enhancement] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) You can specify the `AttributeValue` type (like `"S"`, `"N"`, `"SS"` and so on) used when converting AttributeValue to Embulk type.
- [BugFix] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) Avoid `NullPointerException` when Type `N` AttributeValue has `null` in the deprecated operation.
- [Enhancement] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) Examples work without real Dynamodb.
- [Enhancement] [#19](https://github.com/civitaspo/embulk-input-dynamodb/pull/19) Add more examples.
