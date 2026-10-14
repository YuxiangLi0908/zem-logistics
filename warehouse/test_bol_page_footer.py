import io
from pathlib import Path

from django.template.loader import render_to_string
from django.test import SimpleTestCase
from pypdf import PdfReader
from xhtml2pdf import pisa


class BolPageFooterTests(SimpleTestCase):
    def render_pdf(self, name, context=None):
        output = io.BytesIO()
        html = render_to_string("export_file/" + name, context or {"carrier": "test"})
        result = pisa.CreatePDF(html, dest=output)
        self.assertFalse(result.err)
        return PdfReader(output)

    def assert_page_numbers(self, reader):
        total = len(reader.pages)
        for number, page in enumerate(reader.pages, 1):
            self.assertIn(f"{number}/{total}", page.extract_text())

    def test_all_bol_templates_have_page_numbers(self):
        root = Path(__file__).parent / "templates" / "export_file"
        for template in root.glob("*bol*.html"):
            if template.name == "bol_page_footer.html":
                continue
            with self.subTest(template=template.name):
                self.assert_page_numbers(self.render_pdf(template.name))

    def test_packing_list_overflow_counts_every_page(self):
        context = {"pallet": [{"destination": "TEST", "container_number__container_number": str(i),
                              "total_weight": 100, "total_cbm": 2, "total_n_pallet": 1}
                             for i in range(90)]}
        reader = self.render_pdf("bol_template.html", context)
        self.assertGreater(len(reader.pages), 2)
        self.assert_page_numbers(reader)
