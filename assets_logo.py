import base64, urllib.request, pathlib

url = "https://raw.githubusercontent.com/alekseyvz-dotcom/TimesheetTransformer/d212140f513c1d5a382b703c6c7c35b6b3f109d2/logo.png"
data = urllib.request.urlopen(url).read()
b64 = base64.b64encode(data).decode("ascii")

pathlib.Path("assets_logo.py").write_text('LOGO_BASE64 = """\\\n' + b64 + '\n"""', encoding="utf-8")
print("Создано: assets_logo.py")
