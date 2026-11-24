# SPDX-License-Identifier: MIT
import logging
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from typing import Literal

from .config import env

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if env.ACTION_DEBUG else logging.INFO)


@dataclass
class WorkerRequest:
    """ Inputs for the worker process """
    image: bytes
    xml: bytes
    regions: list[str] = field(default_factory=list)


@dataclass
class WorkerResponse:
    """ Worker outputs on success """
    xml: bytes


@dataclass
class WorkerStatus:
    """ Status message from the worker """
    type: Literal['loaded', 'error']
    message: str | None = None


def _send(connection: Connection, message: object) -> bool:
    """ Send a message to the parent. Returns False if the parent is gone. """
    try:
        connection.send(message)
    except OSError:
        logger.warning(f'Could not send {type(message).__name__} to parent')
        return False
    return True


def worker(connection: Connection, spread: float = 0.0) -> None:
    """
    Process-isolated action worker.

    Protocol:
    1. Send exactly one `WorkerStatus` after startup: 'loaded', or 'error' followed by exit if loading failed.
    2. For every request (`request = connection.recv()`), send exactly one `WorkerResponse`
       or `WorkerStatus('error', ...)` (`connection.send(obj)`).
    3. Exit when `None` is received or the parent closes the pipe.
    """
    logger.debug('Initialize worker')

    with connection:
        try:
            import cv2
            import numpy as np
            from atrtools.line_segmentation import LineSegmentation
            from lxml import etree
            from pypxml import PageXML
            
            segmenter = LineSegmentation(spread, threads=env.LINE_SEGMENTATION_THREADS)
            parser = etree.XMLParser(remove_blank_text=True, encoding='utf-8')
        except Exception as exc:
            logger.exception('Failed to load worker')
            _send(connection, WorkerStatus('error', f'{type(exc).__name__} {exc}'))
            return

        if not _send(connection, WorkerStatus('loaded')):
            return

        while True:
            try:
                request = connection.recv()
            except (EOFError, OSError):
                logger.debug('Parent closed pipe; shutting down worker')
                break
            if request is None:
                logger.debug('Parent sent shutdown signal; shutting down worker')
                break

            try:
                if not isinstance(request, WorkerRequest):
                    raise TypeError(f'Unexpected request: {type(request).__name__}')

                xml = PageXML._from_etree(etree.fromstring(request.xml, parser))
                image = cv2.imdecode(
                    np.frombuffer(request.image, np.uint8), 
                    cv2.IMREAD_GRAYSCALE
                )
                if image is None:
                    raise ValueError('Could not decode image')
                if len(np.unique(image)) > 2:
                    raise ValueError('Image is not binary')
                
                if request.regions:
                    result = segmenter.process(image, xml, request.regions)
                else:
                    result = segmenter.process(image, xml)
    
                connection.send(WorkerResponse(etree.tostring(
                    result._to_etree('2024'), 
                    pretty_print=True
                )))

            except Exception as exc:
                logger.exception('Failed to process element')
                if not _send(connection, WorkerStatus('error', f'{type(exc).__name__} {exc}')):
                    break

    logger.info('Worker stopped successfully')
