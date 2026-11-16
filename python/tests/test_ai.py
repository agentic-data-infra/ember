"""ember.ai.generate against a mock OpenAI-compatible server."""

import http.server
import json
import threading
import unittest

import pyarrow as pa

from ember import Session, ai
from ember.functions import col, concat, lit


class _ChatHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        prompt = body["messages"][0]["content"]
        if prompt == "say fail":
            status, reply = 400, b'{"error": "bad prompt"}'
        else:
            status = 200
            reply = json.dumps(
                {"choices": [{"message": {"role": "assistant", "content": prompt.upper()}}]}
            ).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *args):
        pass


class AiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _ChatHandler)
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        url = f"http://127.0.0.1:{cls.server.server_address[1]}/v1"
        cls.ember = (
            Session.builder.appName("PythonAiTest")
            .master("local[2]")
            .config("ember.ai.endpoint.mock.url", url)
            .config("ember.ai.endpoint.mock.model", "test-model")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.ember.stop()
        cls.server.shutdown()

    def test_generate(self):
        df = self.ember.createDataFrame(pa.table({"p": ["a", "fail"]}))
        g = ai.generate(concat(lit("say "), col("p")), endpoint="mock")
        rows = (
            df.select("p", g.getField("result").alias("r"), g.getField("status").alias("s"))
            .orderBy("p")
            .collect()
        )
        self.assertEqual(rows[0], ("a", "SAY A", ""))
        self.assertEqual(rows[1][1], None)
        self.assertTrue(rows[1][2].startswith("HTTP 400"))


if __name__ == "__main__":
    unittest.main()
