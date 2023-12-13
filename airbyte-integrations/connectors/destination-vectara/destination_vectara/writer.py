#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import dpath.util
import uuid

from typing import Any, Dict, List, Mapping, Optional

from airbyte_cdk.models import ConfiguredAirbyteCatalog, AirbyteRecordMessage, ConfiguredAirbyteStream
from airbyte_cdk.models.airbyte_protocol import DestinationSyncMode
from airbyte_cdk.utils.traced_exception import AirbyteTracedException, FailureType

from destination_vectara.client import VectaraClient



METADATA_STREAM_FIELD = "_ab_stream"
# METADATA_RECORD_ID_FIELD = "_ab_record_id"

class VectaraWriter:

    write_buffer: List[Mapping[str, Any]] = [] #TODO fix
    flush_interval = 1000

    def __init__(self, client: VectaraClient, text_fields: Optional[List[str]], metadata_fields: Optional[List[str]], catalog: ConfiguredAirbyteCatalog):
        self.client = client
        self.text_fields = text_fields
        self.metadata_fields = metadata_fields
        self.streams = {f"{stream.stream.namespace}_{stream.stream.name}": stream for stream in catalog.streams}
        self.ids_to_delete: List[str]

    def delete_streams_to_overwrite(self, catalog: ConfiguredAirbyteCatalog) -> None:
        streams_to_overwrite = [
            stream.stream.name for stream in catalog.streams if stream.destination_sync_mode == DestinationSyncMode.overwrite
        ]
        if len(streams_to_overwrite):
            self.client.delete_doc_by_metadata(metadata_field_name=METADATA_STREAM_FIELD, metadata_field_values=streams_to_overwrite)

    def _delete_documents_to_dedupe(self):
        if len(self.ids_to_delete) > 0:
            self.client.delete_docs_by_id(document_ids=self.ids_to_delete)

    def queue_write_operation(self, record: AirbyteRecordMessage) -> None:
        """Adds messages to the write queue and flushes if the buffer is full"""

        document_section = self._get_document_section(record=record)
        document_metadata = self._get_document_metadata(record=record)
        primary_key = self._get_record_primary_key(record=record)

        document_id = uuid.uuid4().int
        if primary_key:
            document_id = primary_key
            self.ids_to_delete.append(primary_key)

        self.write_buffer.append((document_section, document_metadata, document_id))
        if len(self.write_buffer) == self.flush_interval:
            self.flush()

    def flush(self) -> None:
        """Writes to Convex"""
        self._delete_documents_to_dedupe()
        self.client.index_documents(self.write_buffer)
        self.write_buffer.clear()
        self.ids_to_delete.clear()

    def _get_document_section(self, record: AirbyteRecordMessage):
        relevant_fields = self._extract_relevant_fields(record, self.text_fields)
        if len(relevant_fields) == 0:
            text_fields = ", ".join(self.text_fields) if self.text_fields else "all fields"
            raise AirbyteTracedException(
                internal_message="No text fields found in record",
                message=f"Record {str(record.data)[:250]}... does not contain any of the configured text fields: {text_fields}. Please check your processing configuration, there has to be at least one text field set in each record.",
                failure_type=FailureType.config_error,
            )
        document_section = relevant_fields
        return document_section
    
    def _extract_relevant_fields(self, record: AirbyteRecordMessage, fields: Optional[List[str]]) -> Dict[str, Any]:
        relevant_fields = {}
        if fields and len(fields) > 0:
            for field in fields:
                values = dpath.util.values(record.data, field, separator=".")
                if values and len(values) > 0:
                    relevant_fields[field] = values if len(values) > 1 else values[0]
        else:
            relevant_fields = record.data
        return relevant_fields
    

    def _get_document_metadata(self, record: AirbyteRecordMessage) -> Dict[str, Any]:        
        document_metadata = self._extract_relevant_fields(record, self.metadata_fields)
        document_metadata[METADATA_STREAM_FIELD] = record.stream
        return document_metadata
    
    def _get_record_primary_key(self, record: AirbyteRecordMessage) -> Optional[str]:
        stream_identifier = f"{record.namespace}_{record.stream}"
        current_stream: ConfiguredAirbyteStream = self.streams[stream_identifier]
        if not current_stream.primary_key:
            return
            raise AirbyteTracedException(
                internal_message="No primary key found in current stream",
                message=f"Stream {stream_identifier}... does not contain any configured primary key path. Please check your source stream, there has to be a primary key path configured.",
                failure_type=FailureType.config_error,
            )

        primary_key = []
        for key in current_stream.primary_key:
            try:
                primary_key.append(str(dpath.util.get(record.data, key)))
            except KeyError:
                primary_key.append("__not_found__")
        stringified_primary_key = "_".join(primary_key)
        return f"{stream_identifier}_{stringified_primary_key}"

