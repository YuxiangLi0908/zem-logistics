"""Database-free response transport regression tests."""

import gzip
import unittest

from asgiref.sync import async_to_sync
from django.http import HttpResponse, StreamingHttpResponse
from django.test import RequestFactory

from warehouse.response_delivery import LargePageResponseMiddleware


class LargePageResponseTests(unittest.TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.body = ('<tr><td>库存排约</td><td>CAIU4345316</td></tr>' * 5000).encode()

    def response(self):
        response = HttpResponse(self.body)
        response['Content-Length'] = str(len(self.body))
        response['Vary'] = 'Cookie'
        response.set_cookie('csrftoken', 'unchanged')
        return response

    def test_compression_preserves_html_cookies_and_cache_variants(self):
        for path in ('/post_nsop/?step=schedule_shipment', '/receivable_accounting/?step=warehouse'):
            with self.subTest(path=path):
                request = self.factory.get(path, HTTP_ACCEPT_ENCODING='gzip, deflate, br')
                with self.assertLogs('warehouse.response_delivery', level='INFO') as logs:
                    response = LargePageResponseMiddleware(lambda request: self.response())(request)
                self.assertEqual(gzip.decompress(response.content), self.body)
                self.assertLess(len(response.content), len(self.body) // 10)
                self.assertEqual(int(response['Content-Length']), len(response.content))
                self.assertEqual(response.cookies['csrftoken'].value, 'unchanged')
                self.assertIn('Cookie', response['Vary'])
                self.assertIn('Accept-Encoding', response['Vary'])
                self.assertIn('response_ready', logs.output[0])

    def test_client_without_gzip_keeps_original_body(self):
        response = LargePageResponseMiddleware(lambda request: self.response())(
            self.factory.get('/post_nsop/')
        )
        self.assertEqual(response.content, self.body)
        self.assertNotIn('Content-Encoding', response)

    def test_unrelated_routes_downloads_streams_and_encoded_responses_unchanged(self):
        download = self.response()
        download['Content-Disposition'] = 'attachment; filename="report.html"'
        encoded = self.response()
        encoded['Content-Encoding'] = 'br'
        stream = StreamingHttpResponse(iter([self.body]))
        for path, original in (
            ('/health/', self.response()),
            ('/post_nsop/', download),
            ('/post_nsop/', stream),
            ('/post_nsop/', encoded),
            ('/post_nsop/', HttpResponse(self.body, content_type='application/pdf')),
        ):
            with self.subTest(path=path, headers=dict(original.headers)):
                headers = dict(original.headers)
                body = None if original.streaming else original.content
                response = LargePageResponseMiddleware(lambda request: original)(
                    self.factory.get(path, HTTP_ACCEPT_ENCODING='gzip')
                )
                self.assertIs(response, original)
                self.assertEqual(dict(response.headers), headers)
                if not response.streaming:
                    self.assertEqual(response.content, body)

    def test_async_post_response_preserves_content(self):
        async def get_response(request):
            return self.response()

        request = self.factory.post('/post_nsop/?step=schedule_shipment',
                                    {'step': 'fleet_confirmation'}, HTTP_ACCEPT_ENCODING='gzip')
        response = async_to_sync(LargePageResponseMiddleware(get_response))(request)
        self.assertEqual(gzip.decompress(response.content), self.body)


if __name__ == '__main__':
    unittest.main()
