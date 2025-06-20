"""Async Firestore helpers using grpc.aio"""

import asyncio, grpc
from google.cloud.firestore_v1.services.firestore.transports.grpc_asyncio import FirestoreGrpcAsyncIOTransport
from google.cloud.firestore_v1.types import (
    ListenRequest, Target, StructuredQuery, RunQueryRequest, Value, ArrayValue, TargetChange
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

    async def listen_favourites(self, device_ids: List[str]) -> AsyncIterator[Dict]:
        """Realtime stream of favourites documents (additions & removals) with automatic reconnection."""

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
            order_by=[
                StructuredQuery.Order(
                    field=StructuredQuery.FieldReference(field_path="created_at"),
                    direction=StructuredQuery.Direction.DESCENDING,
                )
            ],
            limit=Int32Value(value=50),
        )

        listen_stub = self._transport._stubs["listen"]

        cache: dict[str, str] = {}  # doc_name -> show_alias

        attempt = 0

        while True:
            attempt += 1
            tgt_id = 1000 + attempt  # unique id each connect to avoid server confusion
            target = Target(target_id=tgt_id, query=Target.QueryTarget(parent=parent_docs, structured_query=structured))

            async def req_iter():
                yield ListenRequest(database=self.database_path, add_target=target)
                while True:
                    await asyncio.sleep(240)
                    yield ListenRequest(database=self.database_path)

            try:
                async for resp in listen_stub(req_iter(), metadata=self._metadata_database()):
                    typ = resp._pb.WhichOneof("response_type")

                    if typ == "document_change":
                        doc = resp.document_change.document
                        alias = doc.fields["show_alias"].string_value
                        cache[doc.name] = alias
                        created_at = doc.fields["created_at"].timestamp_value
                        yield {
                            "op": "ADDED",
                            "show_alias": alias,
                            "created_at": created_at,
                        }

                    elif typ in ("document_delete", "document_remove"):
                        doc_name = (
                            resp.document_delete.document
                            if typ == "document_delete"
                            else resp.document_remove.document
                        )
                        alias = cache.pop(doc_name, "")
                        yield {
                            "op": "REMOVED",
                            "show_alias": alias,
                            "created_at": None,
                        }
            except grpc.aio.AioRpcError as exc:
                if exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.INTERNAL):
                    # Reconnect after brief pause, generating a new target id.
                    await asyncio.sleep(2)
                    continue
                raise

    # ----- favourite episodes -----------------------------------------

    async def query_favourite_episodes(self, device_ids: List[str], limit: int = 50) -> List[Dict]:
        docs = await self.query_favourites(device_ids, limit*2)
        eps = [d for d in docs if "episode_alias" in d and d["episode_alias"].string_value]
        return eps[:limit]

    # ----- live tracks -------------------------------------------------

    async def listen_live_tracks(self, pathname: str, *, initial_snapshot: bool = True) -> AsyncIterator[Dict]:
        """Stream live-track documents with automatic reconnection.

        Firestore gRPC streams occasionally drop with UNAVAILABLE / time-outs when
        the connection is idle for too long.  We transparently reconnect and
        continue yielding events so callers can keep listening indefinitely.
        """

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
            order_by=[
                StructuredQuery.Order(
                    field=StructuredQuery.FieldReference(field_path="start_time"),
                    direction=StructuredQuery.Direction.DESCENDING,
                )
            ],
            limit=Int32Value(value=12),
        )

        listen_stub = self._transport._stubs["listen"]

        attempt = 0

        while True:
            attempt += 1
            tgt_id = 2000 + attempt
            target = Target(target_id=tgt_id, query=Target.QueryTarget(parent=parent_docs, structured_query=structured))

            async def req_iter():
                yield ListenRequest(database=self.database_path, add_target=target)
                # heartbeat every 4-5 minutes to keep the stream alive
                while True:
                    await asyncio.sleep(300)
                    yield ListenRequest(database=self.database_path)

            try:
                waiting = not initial_snapshot  # True => want to ignore until first live doc
                saw_current = False

                async for resp in listen_stub(req_iter(), metadata=self._metadata_database()):
                    kind = resp._pb.WhichOneof("response_type")

                    if waiting:
                        if kind == "target_change" and resp.target_change.target_change_type == TargetChange.TargetChangeType.CURRENT:
                            saw_current = True
                            continue
                        if kind == "document_change" and saw_current:
                            waiting = False  # this is first live change
                        else:
                            # still part of initial snapshot – skip
                            continue

                    if kind == "document_change":
                        doc = resp.document_change.document
                        yield {k: v for k, v in doc.fields.items()}
            except grpc.aio.AioRpcError as exc:
                if exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.INTERNAL):
                    await asyncio.sleep(2)
                    continue
                raise

    async def _device_ids(self) -> List[str]:
        # If helper exists use it, otherwise fall back to UID-as-device-id
        if hasattr(self._fs, "get_device_ids_for_user"):
            return await self._fs.get_device_ids_for_user(self._uid)
        return [self._uid]

    # ----- archive plays (history stream) -----------------------------

    async def listen_archive_plays(self, device_ids: List[str]) -> AsyncIterator[Dict]:
        """Stream playback history (archive_plays) with automatic reconnection."""

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

        listen_stub = self._transport._stubs["listen"]

        attempt = 0
        while True:
            attempt += 1
            tgt_id = 3000 + attempt
            target = Target(target_id=tgt_id, query=Target.QueryTarget(parent=parent_docs, structured_query=structured))

            async def req_iter():
                yield ListenRequest(database=self.database_path, add_target=target)
                while True:
                    await asyncio.sleep(240)
                    yield ListenRequest(database=self.database_path)

            try:
                async for resp in listen_stub(req_iter(), metadata=self._metadata_database()):
                    if resp._pb.WhichOneof("response_type") == "document_change":
                        doc = resp.document_change.document
                        yield {k: v for k, v in doc.fields.items()}
            except grpc.aio.AioRpcError as exc:
                if exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.INTERNAL):
                    await asyncio.sleep(2)
                    continue
                raise 