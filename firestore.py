"""Async Firestore helpers using grpc.aio"""

import asyncio, grpc
from google.cloud.firestore_v1.services.firestore.transports.grpc_asyncio import FirestoreGrpcAsyncIOTransport
from google.cloud.firestore_v1.types import (
    ListenRequest, Target, StructuredQuery, RunQueryRequest, Value, ArrayValue
)
from google.protobuf.wrappers_pb2 import Int32Value
from typing import AsyncIterator, List, Dict
from google.auth.credentials import AnonymousCredentials


class AsyncFirestore:
    def __init__(self, project_id: str, access_token: str):
        self.project_id = project_id
        self.database_path = f"projects/{project_id}/databases/(default)"
        self._channel = grpc.aio.secure_channel("firestore.googleapis.com:443", grpc.ssl_channel_credentials())
        self._transport = FirestoreGrpcAsyncIOTransport(channel=self._channel, credentials=AnonymousCredentials())
        self._access_token = access_token

    def _metadata_parent(self, parent: str):
        return [
            ("authorization", f"Bearer {self._access_token}"),
            ("google-cloud-resource-prefix", self.database_path),
            ("x-goog-request-params", f"parent={parent}"),
        ]

    def _metadata_database(self):
        return [
            ("authorization", f"Bearer {self._access_token}"),
            ("google-cloud-resource-prefix", self.database_path),
            ("x-goog-request-params", f"database={self.database_path}"),
        ]

    # ----- favourites --------------------------------------------------

    async def query_favourites(self, device_ids: List[str], limit: int = 50) -> List[Dict]:
        parent_docs = self.database_path + "/documents"
        comp = StructuredQuery.Filter(
            field_filter=StructuredQuery.FieldFilter(
                field=StructuredQuery.FieldReference(field_path="device_id"),
                op=StructuredQuery.FieldFilter.Operator.IN,
                value=Value(array_value=ArrayValue(values=[Value(string_value=d) for d in device_ids])),
            )
        )
        structured = StructuredQuery(
            from_=[StructuredQuery.CollectionSelector(collection_id="favourites")],
            where=comp,
            order_by=[StructuredQuery.Order(field=StructuredQuery.FieldReference(field_path="created_at"), direction=StructuredQuery.Direction.DESCENDING)],
            limit=Int32Value(value=limit),
        )
        req = RunQueryRequest(parent=parent_docs, structured_query=structured)
        stub = self._transport._stubs["run_query"]
        favs = []
        async for resp in stub(req, metadata=self._metadata_parent(parent_docs)):
            doc = resp.document
            if doc and doc.fields:
                fields = doc.fields
                favs.append({k: v for k, v in fields.items()})
        return favs

    # ----- favourite episodes -----------------------------------------

    async def query_favourite_episodes(self, device_ids: List[str], limit: int = 50) -> List[Dict]:
        docs = await self.query_favourites(device_ids, limit*2)
        eps = [d for d in docs if "episode_alias" in d and d["episode_alias"].string_value]
        return eps[:limit]

    # ----- live tracks -------------------------------------------------

    async def listen_live_tracks(self, pathname: str) -> AsyncIterator[Dict]:
        parent_docs = self.database_path + "/documents"
        comp = StructuredQuery.Filter(
            field_filter=StructuredQuery.FieldFilter(
                field=StructuredQuery.FieldReference(field_path="stream_pathname"),
                op=StructuredQuery.FieldFilter.Operator.EQUAL,
                value=Value(string_value=pathname),
            )
        )
        structured = StructuredQuery(
            from_=[StructuredQuery.CollectionSelector(collection_id="live_tracks")],
            where=comp,
            order_by=[StructuredQuery.Order(field=StructuredQuery.FieldReference(field_path="start_time"), direction=StructuredQuery.Direction.DESCENDING)],
            limit=Int32Value(value=12),
        )
        target = Target(target_id=1, query=Target.QueryTarget(parent=parent_docs, structured_query=structured))
        listen_req = ListenRequest(database=self.database_path, add_target=target)
        stub = self._transport._stubs["listen"]

        async def req_iter():
            yield listen_req
            while True:
                await asyncio.sleep(300)
                yield ListenRequest(database=self.database_path)

        async for resp in stub(req_iter(), metadata=self._metadata_database()):
            kind = resp._pb.WhichOneof("response_type")
            if kind == "document_change":
                doc = resp.document_change.document
                yield {k: v for k, v in doc.fields.items()}

    async def _device_ids(self) -> List[str]:
        # If helper exists use it, otherwise fall back to UID-as-device-id
        if hasattr(self._fs, "get_device_ids_for_user"):
            return await self._fs.get_device_ids_for_user(self._uid)
        return [self._uid]

    # ----- archive plays (history stream) -----------------------------

    async def listen_archive_plays(self, device_ids: List[str]) -> AsyncIterator[Dict]:
        """Stream playback history (archive_plays) for given device_ids in real time."""

        parent_docs = self.database_path + "/documents"

        comp = StructuredQuery.Filter(
            field_filter=StructuredQuery.FieldFilter(
                field=StructuredQuery.FieldReference(field_path="device_id"),
                op=StructuredQuery.FieldFilter.Operator.IN,
                value=Value(array_value=ArrayValue(values=[Value(string_value=d) for d in device_ids])),
            )
        )

        structured = StructuredQuery(
            from_=[StructuredQuery.CollectionSelector(collection_id="archive_plays")],
            where=comp,
            order_by=[
                StructuredQuery.Order(
                    field=StructuredQuery.FieldReference(field_path="played_at"),
                    direction=StructuredQuery.Direction.DESCENDING,
                )
            ],
            limit=Int32Value(value=10),
        )

        target = Target(target_id=10, query=Target.QueryTarget(parent=parent_docs, structured_query=structured))
        listen_req = ListenRequest(database=self.database_path, add_target=target)

        stub = self._transport._stubs["listen"]

        async def req_iter():
            yield listen_req
            while True:
                await asyncio.sleep(240)
                yield ListenRequest(database=self.database_path)

        async for resp in stub(req_iter(), metadata=self._metadata_database()):
            if resp._pb.WhichOneof("response_type") == "document_change":
                doc = resp.document_change.document
                yield {k: v for k, v in doc.fields.items()} 