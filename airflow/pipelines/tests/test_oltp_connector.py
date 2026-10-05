import unittest
from unittest.mock import AsyncMock, Mock, patch

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from src.connections.oltp_connector import OltpConnector

from support import oltp_settings


class OltpConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_settings_and_lifecycle(self):
        settings = oltp_settings()
        engine = AsyncMock(spec=AsyncEngine)
        session = AsyncMock(spec=AsyncSession)
        factory = Mock(return_value=session)
        with (
            patch("src.connections.oltp_connector.create_async_engine", return_value=engine) as create,
            patch("src.connections.oltp_connector.async_sessionmaker", return_value=factory) as sessionmaker,
            patch("src.connections.oltp_connector.OltpSettings") as defaults,
        ):
            connector = OltpConnector(settings)
            async with connector as opened:
                self.assertIs(opened, connector)
                async with opened.session() as actual:
                    self.assertIs(actual, session)
                session.close.assert_awaited_once()
                engine.dispose.assert_not_awaited()
            create.assert_called_once_with(url=settings.database_url)
            sessionmaker.assert_called_once_with(engine, expire_on_commit=False)
            defaults.assert_not_called()
            engine.dispose.assert_awaited_once()
            self.assertIsNone(connector.engine)
            self.assertIsNone(connector.session_factory)

    async def test_default_settings_are_lazy(self):
        settings = oltp_settings()
        with (
            patch("src.connections.oltp_connector.OltpSettings", return_value=settings) as defaults,
            patch("src.connections.oltp_connector.create_async_engine", return_value=AsyncMock(spec=AsyncEngine)),
        ):
            connector = OltpConnector()
            defaults.assert_not_called()
            async with connector:
                defaults.assert_called_once_with()

    async def test_errors_close_session_and_engine(self):
        engine = AsyncMock(spec=AsyncEngine)
        session = AsyncMock(spec=AsyncSession)
        error = ValueError("work failed")
        with (
            patch("src.connections.oltp_connector.create_async_engine", return_value=engine),
            patch("src.connections.oltp_connector.async_sessionmaker", return_value=Mock(return_value=session)),
        ):
            connector = OltpConnector(oltp_settings())
            with self.assertRaises(ValueError) as caught:
                async with connector:
                    async with connector.session():
                        raise error
            self.assertIs(caught.exception, error)
            session.close.assert_awaited_once()
            engine.dispose.assert_awaited_once()

    async def test_session_requires_open_connector(self):
        connector = OltpConnector()
        with self.assertRaisesRegex(RuntimeError, "not open"):
            async with connector.session():
                self.fail("An unopened connector yielded a session")

    async def test_double_open_is_rejected(self):
        engine = AsyncMock(spec=AsyncEngine)
        with patch("src.connections.oltp_connector.create_async_engine", return_value=engine) as create:
            connector = OltpConnector(oltp_settings())
            async with connector:
                with self.assertRaisesRegex(RuntimeError, "already open"):
                    await connector.__aenter__()
            create.assert_called_once()

    async def test_dispose_error_resets_connector(self):
        engine = AsyncMock(spec=AsyncEngine)
        engine.dispose.side_effect = RuntimeError("dispose failed")
        with patch("src.connections.oltp_connector.create_async_engine", return_value=engine):
            connector = OltpConnector(oltp_settings())
            with self.assertRaisesRegex(RuntimeError, "dispose failed"):
                async with connector:
                    pass
            self.assertIsNone(connector.engine)
            self.assertIsNone(connector.session_factory)
