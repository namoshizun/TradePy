import http.server
import socketserver

BaseHandler = http.server.SimpleHTTPRequestHandler


PORT = 8000
PATH = ""
DOC_DIR = "build/html"


class DocHTTPRequestHandler(BaseHandler):
    def __init__(self, *args, **kwargs) -> None:
        kwargs["directory"] = DOC_DIR
        super().__init__(*args, **kwargs)

    def end_headers(self):
        self.send_no_cache_headers()
        super().end_headers()

    def send_no_cache_headers(self):
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")


with socketserver.TCPServer((PATH, PORT), DocHTTPRequestHandler) as httpd:
    print(f"Serving at http://localhost:{PORT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Keyboard interrupted. Closing down")
