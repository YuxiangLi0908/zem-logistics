"""Add page numbers after BOL layout and PDF merging are complete."""
import io

from PyPDF2 import PdfReader, PdfWriter, Transformation
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
from reportlab.pdfgen import canvas


def number_bol_pdf(data):
    reader = PdfReader(io.BytesIO(data))
    writer = PdfWriter()
    if reader.metadata:
        writer.add_metadata({key: str(value) for key, value in reader.metadata.items() if value is not None})
    if "STSong-Light" not in pdfmetrics.getRegisteredFontNames():
        pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    total = len(reader.pages)
    overlays = []
    for number, page in enumerate(reader.pages, 1):
        box = page.cropbox
        left, bottom = float(box.left), float(box.bottom)
        width, height = float(box.width), float(box.height)
        rotation = int(page.get("/Rotate", 0)) % 360
        visual_width, visual_height = (height, width) if rotation in (90, 270) else (width, height)
        overlay = io.BytesIO()
        footer = canvas.Canvas(overlay, pagesize=(visual_width, visual_height))
        label = f"{number}/{total}页"
        footer.setFillColorRGB(0, 0, 0)
        footer.setStrokeColorRGB(0, 0, 0)
        footer.setLineWidth(0.3)
        text = footer.beginText(visual_width - 18 - pdfmetrics.stringWidth(label, "STSong-Light", 9), 12)
        text.setFont("STSong-Light", 9)
        # Fill and stroke makes the Chinese page label bold as well as the numbers.
        text.setTextRenderMode(2)
        text.textOut(label)
        footer.drawText(text)
        footer.save()
        transforms = {
            0: (1, 0, 0, 1, left, bottom),
            90: (0, 1, -1, 0, left + width, bottom),
            180: (-1, 0, 0, -1, left + width, bottom + height),
            270: (0, -1, 1, 0, left, bottom + height),
        }
        overlay_reader = PdfReader(overlay)
        overlays.append(overlay_reader)
        stamp = overlay_reader.pages[0]
        stamp.add_transformation(Transformation(transforms[rotation]))
        # Keep the overlay clipping box in the original page's coordinate system.
        stamp.mediabox = page.mediabox
        stamp.cropbox = page.cropbox
        page.merge_page(stamp, expand=False)
        writer.add_page(page)
    output = io.BytesIO()
    writer.write(output)
    return output.getvalue()


def number_bol_response(response):
    response.content = number_bol_pdf(response.content)
    if response.has_header("Content-Length"):
        response["Content-Length"] = str(len(response.content))
    return response
