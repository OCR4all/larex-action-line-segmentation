# SPDX-License-Identifier: MIT
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from multiprocessing.connection import Connection

from .config import env

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if env.ACTION_DEBUG else logging.INFO)


@dataclass
class WorkerRequest:
    """ Worker inputs """
    image: bytes
    xml: bytes
    regions: list[str] = field(default_factory=list)
    

@dataclass
class WorkerSuccess:
    """ Worker outputs on success """
    xml: bytes
    

@dataclass
class WorkerFailure:
    """ Worker output on failure """
    error: str


def worker(connection: Connection, spread: float = 0.0) -> None:
    """
    Process-isolated action worker.
    
    Request input with `request = connection.recv()`.
    Send output with `connection.send(obj)`.
    """
    logger.debug('Initialize worker')
    
    import cv2
    import numpy as np
    from atrtools.line_segmentation import LineSegmentation
    from lxml import etree
    from pypxml import PageXML
    
    segmenter = LineSegmentation(spread, threads=env.LINE_SEGMENTATION_THREADS)
    parser = etree.XMLParser(remove_blank_text=True, encoding='utf-8')
    
    while True:
        try:
            request = connection.recv()
        except EOFError:
            logger.debug('Parent closed pipe; shutting down worker')
            break
        if request is None:
            logger.debug('Parent sent shutdown signal; shutting down worker')
            break   
        if not isinstance(request, WorkerRequest):
            logger.error(f'Received unexpected request: {type(request).__name__}')
            continue
        
        try:
            xml = PageXML._from_etree(etree.fromstring(request.xml, parser))
            image = cv2.imdecode(np.frombuffer(request.image, np.uint8), cv2.IMREAD_GRAYSCALE)
            if image is None:
                raise ValueError('Could not decode image')
            if len(np.unique(image)) > 2:
                raise ValueError('Image is not binary')
            
            if request.regions:
                result = segmenter.process(image, xml, request.regions)
            else:
                result = segmenter.process(image, xml)

            connection.send(WorkerSuccess(etree.tostring(result._to_etree(), pretty_print=True)))
            
        except Exception as exc:
            logger.exception('Failed to process element')
            try:
                connection.send(WorkerFailure(f'{type(exc).__name__}: {exc}'))
            except (BrokenPipeError, EOFError, OSError):
                logger.warning('Could not send worker failure to parent')
                break
    
    connection.close()
    logger.info('Worker stopped successfully')
