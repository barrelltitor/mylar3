#  This file is part of Mylar.
#
#  Provides a lightweight wrapper around the JDownloader2 remote API so
#  GetComics links can be handed off for downloading.

import json
import os
import datetime
from typing import Any, Dict, List, Optional

import requests

import mylar
from mylar import db, logger


class JDownloader2(object):
    """Simple helper for submitting links to JDownloader2 and polling status."""

    LINKGRABBER_ENDPOINT = 'linkgrabberv2/addLinks'
    DOWNLOADS_ENDPOINT = 'downloadsV2/queryLinks'

    def __init__(self, base_url: Optional[str] = None, timeout: int = 30, session: Optional[requests.Session] = None):
        self.base_url = (base_url or mylar.CONFIG.JD2_URL or '').rstrip('/')
        if not self.base_url:
            raise ValueError('JD2 URL is not configured')
        self.timeout = timeout
        self.session = session or requests.Session()

        self.destination_root = mylar.CONFIG.JD2_DEST_DIR
        self.destination_folder = None
        if self.destination_root:
            self.destination_folder = self.destination_root
            try:
                os.makedirs(self.destination_folder, exist_ok=True)
            except Exception as err:
                logger.warn('[JD2] Unable to ensure destination folder exists: %s', err)
                self.destination_folder = None

    def _url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def submit(self, link: str, package_name: str, autostart: bool = True, record_id: Optional[str] = None) -> Dict[str, Any]:
        """Submit a link to JD2 and return the assigned job id (if any)."""
        query = {
            'assignJobID': True,
            'autostart': autostart,
            'links': link,
            'packageName': package_name,
        }
        if self.destination_folder:
            query['destinationFolder'] = self.destination_folder
        endpoint = self._url(self.LINKGRABBER_ENDPOINT)
        params = {'query': json.dumps(query)}
        try:
            resp = self.session.get(
                endpoint,
                params=params,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except Exception as err:
            logger.error('[JD2] Failed to submit %s (url=%s params=%s): %s', package_name, endpoint, params, err)
            return {'status': False, 'jobid': None, 'error': err}

        job_id = None
        try:
            payload = resp.json() or {}
        except Exception as json_err:
            logger.warn('[JD2] Unable to decode submit response JSON: %s', json_err)
            payload = {}

        if isinstance(payload, dict):
            data = payload.get('data', payload)
            if isinstance(data, dict):
                job_id = data.get('id') or data.get('jobID')

        if job_id is None:
            logger.warn('[JD2] No job id returned for %s', package_name)
        else:
            job_key = str(job_id)
            if record_id:
                try:
                    myDB = db.DBConnection()
                    myDB.upsert(
                        'ddl_info',
                        {
                            'jd2_job_id': job_key,
                            'status': 'JD2-Submitted',
                            'updated_date': datetime.datetime.now().strftime('%Y-%m-%d %H:%M'),
                        },
                        {'id': record_id},
                    )
                except Exception as err:
                    logger.warn('[JD2] Unable to persist job id %s for record %s: %s', job_key, record_id, err)
        return {'status': True, 'jobid': str(job_id) if job_id is not None else None, 'payload': payload}

    def query(self, job_ids: List[str]) -> List[Dict[str, Any]]:
        if not job_ids:
            return []
        query = {
            'jobUUID': True,
            'jobUUIDs': job_ids,
            'status': True,
            'maxResults': 1000,
            'startAt': 0,
        }
        endpoint = self._url(self.DOWNLOADS_ENDPOINT)
        params = {'queryParams': json.dumps(query)}
        try:
            resp = self.session.get(
                endpoint,
                params=params,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except Exception as err:
            logger.error('[JD2] Failed to query job ids %s (url=%s params=%s): %s', job_ids, endpoint, params, err)
            return []

        try:
            payload = resp.json() or {}
        except Exception as json_err:
            logger.warn('[JD2] Unable to decode query response JSON: %s', json_err)
            payload = {}


        data = payload.get('data', payload if isinstance(payload, list) else [])
        return data if isinstance(data, list) else []

    def status(self, job_id: str) -> Dict[str, Any]:
        """Return the JD2 status for the given job id."""
        if job_id is None:
            return {'found': False, 'status': None, 'data': None}
        packages = self.query([job_id])
        if not packages:
            return {'found': False, 'status': None, 'data': None}
        package = packages[0]
        status = package.get('status') if isinstance(package, dict) else None
        return {'found': True, 'status': status, 'data': package}
