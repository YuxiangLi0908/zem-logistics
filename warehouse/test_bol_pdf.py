import io
from pathlib import Path

from django.template.loader import render_to_string
from django.test import SimpleTestCase
from PyPDF2 import PdfReader, PdfWriter
from xhtml2pdf import pisa

from warehouse.utils.bol_pdf import number_bol_pdf


class BolPdfNumberTests(SimpleTestCase):
    def assert_preserved(self, original, numbered):
        self.assertEqual(len(original.pages), len(numbered.pages))
        total = len(original.pages)
        for index, (before, after) in enumerate(zip(original.pages, numbered.pages), 1):
            self.assertEqual(list(before.mediabox), list(after.mediabox))
            self.assertEqual(list(before.cropbox), list(after.cropbox))
            self.assertEqual(before.rotation, after.rotation)
            self.assertIn(f"{index}/{total}", after.extract_text().replace("\x00", ""))
            # Original text retains exactly the same positioning matrices.
            def positions(page):
                found = []
                page.extract_text(visitor_text=lambda text, cm, tm, font, size:
                                  found.append((text, tuple(cm), tuple(tm), size)) if text.strip() else None)
                return found
            self.assertEqual(positions(before), positions(after)[:len(positions(before))])

    def test_all_templates_keep_layout(self):
        root = Path(__file__).parent / "templates" / "export_file"
        for path in root.glob("*bol*.html"):
            with self.subTest(template=path.name):
                html = render_to_string("export_file/" + path.name, {"carrier": "test"})
                source = io.BytesIO()
                self.assertFalse(pisa.CreatePDF(html, dest=source).err)
                numbered = number_bol_pdf(source.getvalue())
                self.assert_preserved(PdfReader(source), PdfReader(io.BytesIO(numbered)))

    def test_merged_pages_with_rotation_and_crop_keep_layout(self):
        writer = PdfWriter()
        for rotation in (0, 90, 180, 270):
            page = writer.add_blank_page(width=612, height=792)
            page.cropbox.lower_left = (20, 30)
            page.cropbox.upper_right = (592, 762)
            page.rotate(rotation)
        source = io.BytesIO()
        writer.write(source)
        numbered = number_bol_pdf(source.getvalue())
        self.assert_preserved(PdfReader(source), PdfReader(io.BytesIO(numbered)))

    def test_long_packing_list_keeps_pagination(self):
        context = {"pallet": [{"destination": "TEST", "container_number__container_number": str(i),
                              "total_weight": 100, "total_cbm": 2, "total_n_pallet": 1}
                             for i in range(90)]}
        source = io.BytesIO()
        html = render_to_string("export_file/bol_template.html", context)
        self.assertFalse(pisa.CreatePDF(html, dest=source).err)
        original = PdfReader(source)
        self.assertGreater(len(original.pages), 2)
        self.assert_preserved(original, PdfReader(io.BytesIO(number_bol_pdf(source.getvalue()))))
