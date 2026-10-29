package com.example.dsf.config

import jakarta.annotation.PostConstruct
import org.flywaydb.core.Flyway
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Configuration
import javax.sql.DataSource

@Configuration
class DatabaseMigrationConfig(
    @Qualifier("dataSource") private val postgresDataSource: DataSource,
    @Qualifier("clickhouseDataSource") private val clickhouseDataSource: DataSource,
    @Value($$"${app.demo-data.enabled:false}") private val demoDataEnabled: Boolean,
) {
    @PostConstruct
    fun migrateDatabases() {
        Flyway
            .configure()
            .dataSource(postgresDataSource)
            .locations("classpath:db/migration/postgres")
            .baselineOnMigrate(true)
            .load()
            .migrate()

        Flyway
            .configure()
            .dataSource(clickhouseDataSource)
            .locations("classpath:db/migration/clickhouse")
            .baselineOnMigrate(true)
            .load()
            .migrate()

        if (demoDataEnabled) {
            seedDemoData()
        }
    }

    /**
     * Inserts the demo orders of the scenario runbook, once per database.
     *
     * Only PostgreSQL is seeded (orders, status timeline and outbox): Debezium and the consumer write the order
     * history, so its versions are real Kafka offsets and later repairs always get a higher version. The seed has its
     * own history table, so its version numbers never collide with the schema migrations; the baseline version 0 lets
     * the first seed run on the already migrated, non-empty schema.
     */
    private fun seedDemoData() {
        Flyway
            .configure()
            .dataSource(postgresDataSource)
            .locations("classpath:db/seed/postgres")
            .table("flyway_demo_data_history")
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load()
            .migrate()
    }
}
