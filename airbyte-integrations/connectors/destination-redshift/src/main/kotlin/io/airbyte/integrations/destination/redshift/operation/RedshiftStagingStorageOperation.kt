package io.airbyte.integrations.destination.redshift.operation

import io.airbyte.cdk.integrations.base.JavaBaseConstants
import io.airbyte.cdk.integrations.destination.record_buffer.SerializableBuffer
import io.airbyte.cdk.integrations.destination.s3.AesCbcEnvelopeEncryptionBlobDecorator
import io.airbyte.cdk.integrations.destination.s3.S3StorageOperations
import io.airbyte.cdk.integrations.destination.staging.SerialStagingConsumerFactory
import io.airbyte.integrations.base.destination.operation.StorageOperation
import io.airbyte.integrations.base.destination.typing_deduping.Sql
import io.airbyte.integrations.base.destination.typing_deduping.StreamConfig
import io.airbyte.integrations.base.destination.typing_deduping.StreamId
import io.airbyte.integrations.base.destination.typing_deduping.TyperDeduperUtil
import io.airbyte.integrations.destination.redshift.RedshiftSQLNameTransformer
import io.airbyte.integrations.destination.redshift.typing_deduping.RedshiftDestinationHandler
import io.airbyte.integrations.destination.redshift.typing_deduping.RedshiftSqlGenerator
import io.airbyte.protocol.models.v0.DestinationSyncMode
import io.github.oshai.kotlinlogging.KotlinLogging
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.util.Optional
import java.util.UUID

private val log = KotlinLogging.logger {}

/**
 * @param keyEncryptingKey The KEK to use when writing to S3, or null if encryption is disabled.
 * If this parameter is nonnull, then `s3StorageOperations` MUST have an
 * [AesCbcEnvelopeEncryptionBlobDecorator] added (via `s3StorageOperations#addBlobDecorator`).
 */
class RedshiftStagingStorageOperation(
    private val bucketPath: String,
    private val keepStagingFiles: Boolean,
    private val s3StorageOperations: S3StorageOperations,
    private val sqlGenerator: RedshiftSqlGenerator,
    private val destinationHandler: RedshiftDestinationHandler,
    private val keyEncryptingKey: ByteArray?,
    private val connectionId: UUID = SerialStagingConsumerFactory.RANDOM_CONNECTION_ID,
    private val writeDatetime: ZonedDateTime = Instant.now().atZone(ZoneOffset.UTC),
): StorageOperation<SerializableBuffer> {
    override fun prepareStage(streamId: StreamId, destinationSyncMode: DestinationSyncMode) {
        // create raw table
        destinationHandler.execute(Sql.of(createRawTableQuery(streamId)))
        if (destinationSyncMode == DestinationSyncMode.OVERWRITE) {
            destinationHandler.execute(Sql.of(truncateRawTableQuery(streamId)))
        }
        // create bucket for staging files
        s3StorageOperations.createBucketIfNotExists()
    }

    override fun writeToStage(streamId: StreamId, data: SerializableBuffer) {
        val objectPath: String = getStagingPath(streamId)
        log.info {
            "Uploading records to for ${streamId.rawNamespace}.${streamId.rawName} to path $objectPath"
        }
        s3StorageOperations.uploadRecordsToBucket(data, streamId.rawNamespace, objectPath)
    }

    override fun cleanupStage(streamId: StreamId) {
        if (keepStagingFiles) return
        val stagingRootPath = getStagingPath(streamId)
        log.info { "Cleaning up staging path at $stagingRootPath" }
        s3StorageOperations.dropBucketObject(stagingRootPath)
    }

    override fun createFinalTable(streamConfig: StreamConfig, suffix: String, replace: Boolean) {
        destinationHandler.execute(sqlGenerator.createTable(streamConfig, suffix, replace))
    }

    override fun softResetFinalTable(streamConfig: StreamConfig) {
        TyperDeduperUtil.executeSoftReset(
            sqlGenerator = sqlGenerator,
            destinationHandler = destinationHandler,
            streamConfig,
        )
    }

    override fun overwriteFinalTable(streamConfig: StreamConfig, tmpTableSuffix: String) {
        if (tmpTableSuffix.isNotBlank()) {
            log.info {
                "Overwriting table ${streamConfig.id.finalTableId(RedshiftSqlGenerator.QUOTE)} with ${
                    streamConfig.id.finalTableId(
                        RedshiftSqlGenerator.QUOTE,
                        tmpTableSuffix,
                    )
                }"
            }
            destinationHandler.execute(
                sqlGenerator.overwriteFinalTable(streamConfig.id, tmpTableSuffix)
            )
        }
    }

    override fun typeAndDedupe(
        streamConfig: StreamConfig,
        maxProcessedTimestamp: Optional<Instant>,
        finalTableSuffix: String
    ) {
        TyperDeduperUtil.executeTypeAndDedupe(
            sqlGenerator = sqlGenerator,
            destinationHandler = destinationHandler,
            streamConfig,
            maxProcessedTimestamp,
            finalTableSuffix,
        )
    }

    private fun getStagingPath(streamId: StreamId): String {
        val prefix =
            if (bucketPath.isEmpty()) ""
            else bucketPath + (if (bucketPath.endsWith("/")) "" else "/")
        return nameTransformer.applyDefaultCase(
            String.format(
                "%s%s/%s_%02d_%02d_%02d_%s/",
                prefix,
                nameTransformer.applyDefaultCase(
                    // I have no idea why we're doing this.
                    // streamId.rawName already has been passed through the name transformer.
                    nameTransformer.convertStreamName(streamId.rawName)
                ),
                writeDatetime.year,
                writeDatetime.monthValue,
                writeDatetime.dayOfMonth,
                writeDatetime.hour,
                connectionId
            )
        )
    }

    companion object {
        private val nameTransformer = RedshiftSQLNameTransformer()

        private fun createRawTableQuery(streamId: StreamId): String {
            return """
                CREATE TABLE IF NOT EXISTS "${streamId.rawNamespace}"."${streamId.rawName}"
                ${JavaBaseConstants.COLUMN_NAME_AB_RAW_ID} VARCHAR(36),
                ${JavaBaseConstants.COLUMN_NAME_AB_EXTRACTED_AT} TIMESTAMP_WITH_TIMEZONE DEFAULT GETDATE,
                ${JavaBaseConstants.COLUMN_NAME_AB_LOADED_AT} TIMESTAMP_WITH_TIMEZONE,
                ${JavaBaseConstants.COLUMN_NAME_DATA} SUPER NOT NULL,
                ${JavaBaseConstants.COLUMN_NAME_AB_META} SUPER NULL;
            """.trimIndent()
        }

        private fun truncateRawTableQuery(
            streamId: StreamId,
        ): String {
            return String.format("""TRUNCATE TABLE "%s"."%s";\n""", streamId.rawNamespace, streamId.rawName)
        }
    }
}
