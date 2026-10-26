package com.example.dsf.commons

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/** Converts this instant to a [LocalDateTime] on the UTC time-line, independent of the JVM default time zone. */
fun Instant.toUtcLocalDateTime(): LocalDateTime = LocalDateTime.ofInstant(this, ZoneOffset.UTC)

/** Interprets this [LocalDateTime] as UTC and converts it to an [Instant], independent of the JVM default time zone. */
fun LocalDateTime.toUtcInstant(): Instant = toInstant(ZoneOffset.UTC)
