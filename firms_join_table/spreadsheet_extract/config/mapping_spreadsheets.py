from pydantic import BaseModel, computed_field


class SheetSource(BaseModel):
    """A single Google Sheet tab to read."""

    spreadsheet_id: str
    easybill_spreadsheet_name: str
    medisoft_spreadsheet_name: str
    city: str
    description: str = ""

    @computed_field
    @property
    def url(self) -> str:
        return f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}/edit"


class SpreadsheetMapping(BaseModel):
    """Registry of all Google Sheets to read and merge."""

    sheets: list[SheetSource]

    def get(self, city: str) -> SheetSource:
        for sheet in self.sheets:
            if sheet.city == city:
                return sheet
        raise KeyError(f"No sheet with city {city!r}")

    def cities(self) -> list[str]:
        return [s.city for s in self.sheets]


SPREADSHEET_MAPPING = SpreadsheetMapping(
    sheets=[
        SheetSource(
            city="Düsseldorf",
            spreadsheet_id="1Ow24XZ_-v4uU21u4oOKBSfoonVFmPvfxIF7A2e11ILQ",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Düsseldorf_firms",
            description="Easybill and medisoft client lists for Düsseldorf.",
        ),
        SheetSource(
            city="Frankfurt",
            spreadsheet_id="1m4PXY0pAFvU6V0lFqW4erUakcjxJ3bsSsHyXTtbhhRU",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Frankfurt_firms",
            description="Easybill and medisoft client lists for Frankfurt.",
        ),
        SheetSource(
            city="Hamburg",
            spreadsheet_id="1EfzpRkQzkMTqhAIJ9QNUze3Jg61g__yh3uG2D4CNSPk",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Hamburg_firms",
            description="Easybill and medisoft client lists for Hamburg.",
        ),
        SheetSource(
            city="Kiel",
            spreadsheet_id="1OTqVE62shmthvu0KVz4NORFOJr3vG4E-fzfwOKhXg3E",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Kiel_firms",
            description="Easybill and medisoft client lists for Kiel.",
        ),
        SheetSource(
            city="Köln",
            spreadsheet_id="1IEV5HkuLaF9G1WwpS-X9q6OSmde7PJpdnt7O0mOKf7I",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Köln_firms",
            description="Easybill and medisoft client lists for Köln.",
        ),
        SheetSource(
            city="München",
            spreadsheet_id="1jpyDvexJNTG5_n-5NUsteDpcMsQfe6j4yDyrdoQDUmg",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_München_firms",
            description="Easybill and medisoft client lists for Munich.",
        ),
        SheetSource(
            city="Rostock",
            spreadsheet_id="1SBXeTGYQrmQXCCDQz21XGJ9iKE0HM-VZP8r69P5TOZ0",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Rostock_firms",
            description="Easybill and medisoft client lists for Rostock.",
        ),
        SheetSource(
            city="Stuttgart",
            spreadsheet_id="1wYfseil_FbRGmqQ9QwKOREcjgi_FjTTxLQ_n7n2GVA4",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Stuttgart_firms",
            description="Easybill and medisoft client lists for Stuttgart.",
        ),
        SheetSource(
            city="Viersen",
            spreadsheet_id="1ShmRNwBD6AMQtP0BiTuXxhvOg2zXyUEPCoXVQoIhRhE",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_Viersen_firms",
            description="Easybill and medisoft client lists for Viersen.",
        ),
        SheetSource(
            city="Berlin",
            spreadsheet_id="1DynAAKo8sHkGx6TrfSQDlaqyQ5N8jcThXFWGh7k1IlQ",
            easybill_spreadsheet_name="1.Clientlist",
            medisoft_spreadsheet_name="2.medisoft_berlin_firms",
            description="Easybill and medisoft client lists for Berlin.",
        ),
    ]
)
