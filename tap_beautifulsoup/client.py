"""Custom client handling, including BeautifulSoupStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

from bs4 import BeautifulSoup
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream

from tap_beautifulsoup.download import download

class BeautifulSoupStream(Stream):
    """Stream class for BeautifulSoup streams."""

    @property
    def site_url(self) -> str:
        """Return the root URL for the stream."""
        return self.config["site_url"]

    @property
    def site_path(self) -> str:
        """Return the site path glob pattern for the stream."""
        return self.config["site_path"]

    @property
    def output_folder(self) -> Path:
        """Return the download folder for the stream."""
        return Path(self.config["output_folder"])

    @property
    def parser(self) -> str:
        """Return the parser for the stream."""
        return self.config["parser"]

    def download(self) -> None:
        """Download the HTML file for the stream."""
        download(self.site_url, self.output_folder)

    def parse_file(self, file: Path) -> str:
        """Parse the HTML file for the stream.

        Args:
            file: Path to the HTML file.

        Returns:
            The parsed content.
        """
        with open(file, encoding="utf-8") as f:
            data = f.read()

        self.logger.warning(f"PARSING {file} with {self.parser} {str(len(data))}")
        soup = BeautifulSoup(data, features=self.parser)
        text = soup.find_all("body")
        if len(text) != 0:
            text = text[0].get_text()
        else:
            text = ""

        return "\n".join([t for t in text.split("\n") if t])

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.

        Args:
            context: Stream partition or context dictionary.
        """
        self.download()

        docs = []
        path = self.site_url if len(self.site_path) == 0 else self.site_url + "/" + self.site_path
        self.logger.warning(f"GLOB {path}")
        for p in Path(self.output_folder).glob(f"{path}"):
            if p.is_dir():
                continue

            text = self.parse_file(p)
            if not text:
                self.logger.warning(f"Could not find 'body' in file {p}.")

            record = {
                "source": str(p),
                "page_content": text,
                "metadata": {"source": str(p)},
            }
            docs.append(record)
            yield record

        assert len(docs), f"No documents found in {self.output_folder}"

    schema = th.PropertiesList(
        th.Property("source", th.StringType),
        th.Property(
            "page_content",
            th.StringType,
            description="The page content.",
        ),
        th.Property(
            "metadata",
            th.ObjectType(
                th.Property("source", th.StringType),
            ),
        ),
    ).to_dict()
