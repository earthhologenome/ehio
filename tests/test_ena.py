"""Tests for the ENA module: depositing an ENA submission's hologenomes.

ENA is never reached: ena-upload-cli is replaced by a runner that writes the
receipt ENA would have sent, the reads by a fetcher that returns a checksum,
and ENA's records by canned XML.
"""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ehio import cli, ena
from ehio.scanning import MODULES, _sources, build_script_content
from tests.fake_core import FakeCoreClient, using

STUDY_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<PROJECT_SET>
<PROJECT accession="PRJEB76898" alias="5de02048-cb9b-4219-8380-adc0829dbbae" center_name="earth hologenome iniatiative">
  <IDENTIFIERS>
    <PRIMARY_ID>PRJEB76898</PRIMARY_ID>
    <SECONDARY_ID>ERP161386</SECONDARY_ID>
  </IDENTIFIERS>
</PROJECT>
</PROJECT_SET>"""

SAMPLE_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<SAMPLE_SET>
<SAMPLE accession="ERS22455981" alias="AGP08" center_name="Earth Hologenome Iniatiative">
     <IDENTIFIERS>
          <PRIMARY_ID>ERS22455981</PRIMARY_ID>
          <EXTERNAL_ID namespace="BioSample">SAMEA117388399</EXTERNAL_ID>
     </IDENTIFIERS>
</SAMPLE>
</SAMPLE_SET>"""

def lab_sample(code: str, **extra) -> dict:
    """A sample as ehi-core answers it with each hologenome: the core's copy
    of the laboratory's table."""
    return {
        "code": code, "specimen_code": "SD01696", "capture_code": "SD0169601", "sample_type": "Faecal",
        "description": "Faecal sample", "collection_date": "2021-05-19",
        "host_species": "Sciurus vulgaris", "host_common_name": "Eurasian red squirrel",
        "host_taxid": "55149", "host_sex": "Female", "host_life_stage": "adult",
        "country": "Italy", "region": "Lombardy, Varese", "latitude": 45.84, "longitude": 8.81,
        "broad_scale_environment": "1000221 - Temperate woodland",
        "local_environment": "mixed conifer-deciduous wood", "environmental_medium": "animal",
        "taxon_id": "1861841", "ena_sample_accession": None, **extra,
    }


def hologenome(code: str, lab: str, **extra) -> dict:
    """An entry as ehi-core's ena_submissions route answers it."""
    return {
        "code": code, "sample_code": lab, "sample": lab_sample(lab) if lab else None,
        "library_name": f"{lab}E1L1I1", "data_type": "metagenome", "library_source": "METAGENOMIC",
        "platform": "ILLUMINA", "instrument_model": "Illumina NovaSeq 6000",
        "raw_forward_url": f"https://erda/RAW/{code}_1.fq.gz",
        "raw_reverse_url": f"https://erda/RAW/{code}_2.fq.gz",
        "sample_ena_accessions": [], "ena_run_accession": None, **extra,
    }


def receipt(success=True, *, sample=None, experiment=None, run=None, errors=()) -> str:
    parts = [f'<RECEIPT receiptDate="2026-09-22T10:00:00.000+01:00" success="{str(success).lower()}">']
    if sample:
        parts.append(f'<SAMPLE accession="{sample[1]}" alias="{sample[0]}" status="PRIVATE">'
                     f'<EXT_ID accession="SAMEA1" type="biosample"/></SAMPLE>')
    if experiment:
        parts.append(f'<EXPERIMENT accession="{experiment[1]}" alias="{experiment[0]}" status="PRIVATE"/>')
    if run:
        parts.append(f'<RUN accession="{run[1]}" alias="{run[0]}" status="PRIVATE"/>')
    parts.append("<MESSAGES>" + "".join(f"<ERROR>{e}</ERROR>" for e in errors) + "</MESSAGES>")
    parts.append("<ACTIONS>ADD</ACTIONS></RECEIPT>")
    return "".join(parts)


class FakeUploadCli:
    """Stands in for ena-upload-cli: records each call and leaves the receipt
    it is given, the way ena-upload-cli saves ENA's in its working folder."""

    def __init__(self, *receipts: str) -> None:
        self.receipts = list(receipts)
        self.calls: list[tuple[list[str], Path]] = []

    def __call__(self, cmd, folder, creds):
        self.calls.append((cmd, folder))
        if self.receipts:
            answer = self.receipts.pop(0)
            if answer:
                (folder / "receipt.xml").write_text(answer)
        return subprocess.CompletedProcess(cmd, 0, stdout="ena-upload-cli output\n", stderr="")

    def tables(self, call: int = 0) -> list[str]:
        cmd = self.calls[call][0]
        return [flag[2:] for flag in cmd if flag in ("--sample", "--experiment", "--run")]


def fetch(source, dest: Path, timeout) -> str:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"@read\nACGT\n+\nIIII\n")
    return "0" * 32


def aliases(**xml) -> ena.Aliases:
    """ENA's records by accession, as Webin's report service would answer."""
    served = {"PRJEB76898": STUDY_XML, "ERS22455981": SAMPLE_XML, **xml}

    def serve(url, creds, timeout):
        return next((body for accession, body in served.items() if url.endswith(accession)), None)

    return ena.Aliases(ena.Credentials("Webin-1", "pw"), fetch=serve)


CREDS = ena.Credentials("Webin-1", "pw")


@pytest.fixture
def settings(tmp_path):
    return ena.Settings(work_dir=tmp_path / "EHS0001")


def plan_of(entries, settings):
    return ena.plan(
        entries,
        study="PRJEB76898",
        study_alias="5de02048-cb9b-4219-8380-adc0829dbbae",
        settings=settings,
        aliases=aliases(),
    )


# ---------------------------------------------------------------------------
# Values
# ---------------------------------------------------------------------------

class TestText:
    def test_lookup_lists_are_joined(self):
        assert ena.text(["Italy"]) == "Italy"
        assert ena.text(["a", None, "b"]) == "a, b"

    def test_a_whole_number_read_as_a_float_stays_whole(self):
        assert ena.text(55149.0) == "55149"
        assert ena.text(45.84) == "45.84"

    def test_airtable_special_values_and_blanks_are_empty(self):
        assert ena.text({"specialValue": "NaN"}) == ""
        assert ena.text(None) == ""

    def test_tabs_and_line_breaks_become_spaces(self):
        assert ena.text("Faecal\tsample\nfrom a squirrel") == "Faecal sample from a squirrel"


# ---------------------------------------------------------------------------
# ENA's records
# ---------------------------------------------------------------------------

class TestAliases:
    def test_a_study_is_known_by_the_alias_its_project_was_registered_under(self):
        assert ena.alias_in(STUDY_XML, "PRJEB76898") == "5de02048-cb9b-4219-8380-adc0829dbbae"
        assert ena.alias_in(STUDY_XML, "ERP161386") == "5de02048-cb9b-4219-8380-adc0829dbbae"

    def test_a_sample_is_known_by_its_own_alias(self):
        assert ena.alias_in(SAMPLE_XML, "ERS22455981") == "AGP08"
        assert ena.alias_in(SAMPLE_XML, "SAMEA117388399") == "AGP08"

    def test_the_account_is_asked_first_then_the_public_browser(self):
        asked = []

        def serve(url, creds, timeout):
            asked.append((url, creds is not None))
            return SAMPLE_XML if "browser" in url else None

        found = ena.Aliases(CREDS, fetch=serve).of("ERS22455981", "samples")
        assert found == "AGP08"
        assert asked == [
            ("https://www.ebi.ac.uk/ena/submit/report/samples/xml/ERS22455981", True),
            ("https://www.ebi.ac.uk/ena/browser/api/xml/ERS22455981", False),
        ]

    def test_the_test_server_is_asked_about_its_own_records_only(self):
        asked = []
        found = ena.Aliases(CREDS, server=ena.TEST_SERVER,
                            fetch=lambda url, creds, timeout: asked.append(url) or None)
        with pytest.raises(ena.EnaError, match="not in the Webin account"):
            found.of("ERS22455981", "samples")
        assert asked == ["https://wwwdev.ebi.ac.uk/ena/submit/report/samples/xml/ERS22455981"]

    def test_an_alias_is_looked_up_once(self):
        asked = []
        found = ena.Aliases(CREDS, fetch=lambda url, creds, timeout: asked.append(url) or SAMPLE_XML)
        found.of("ERS22455981", "samples")
        found.of("ERS22455981", "samples")
        assert len(asked) == 1


class TestReceipt:
    def test_the_accessions_of_what_was_registered(self):
        got = ena.parse_receipt(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                        run=("ena_EHI00032", "ERR1")).encode())
        assert got.success
        assert got.accessions == {("sample", "AGP08"): "ERS1", ("experiment", "ena_EHI00032"): "ERX1",
                                  ("run", "ena_EHI00032"): "ERR1"}

    def test_what_ena_already_holds_is_read_from_its_refusal(self):
        error = ('In sample, alias: "AGP08". The object being added already exists in the '
                 'submission account with accession: "ERS22455981".')
        got = ena.parse_receipt(receipt(False, errors=[error, "Something else is wrong."]).encode())
        assert not got.success
        assert got.existing == {("sample", "AGP08"): "ERS22455981"}
        assert len(got.errors) == 2


class TestCredentials:
    def test_the_environment_comes_first(self, monkeypatch, tmp_path):
        monkeypatch.setenv("ENA_USERNAME", "Webin-9")
        monkeypatch.setenv("ENA_PASSWORD", "secret")
        assert ena.credentials(str(tmp_path / "none.yml")) == ena.Credentials("Webin-9", "secret")

    def test_the_secret_file_the_cluster_keeps(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ENA_USERNAME", raising=False)
        monkeypatch.delenv("ENA_PASSWORD", raising=False)
        secret = tmp_path / ".secret.yml"
        secret.write_text("username: Webin-1\npassword: pw\n")
        assert ena.credentials(str(secret)) == CREDS

    def test_no_account_at_all(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ENA_USERNAME", raising=False)
        monkeypatch.delenv("ENA_PASSWORD", raising=False)
        assert ena.credentials(str(tmp_path / "missing.yml")) is None

    def test_a_secret_file_without_both_is_refused(self, monkeypatch, tmp_path):
        monkeypatch.delenv("ENA_USERNAME", raising=False)
        monkeypatch.delenv("ENA_PASSWORD", raising=False)
        secret = tmp_path / ".secret.yml"
        secret.write_text("username: Webin-1\n")
        with pytest.raises(ena.EnaError, match="username"):
            ena.credentials(str(secret))


# ---------------------------------------------------------------------------
# Planning a submission
# ---------------------------------------------------------------------------

class TestPlan:
    def test_a_new_lab_sample_is_registered_under_its_code(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08")], settings)
        [sample] = found.samples
        assert (sample.alias, sample.accession) == ("AGP08", None)
        assert sample.row["alias"] == sample.row["title"] == "AGP08"
        assert sample.row["host taxid"] == "55149"
        assert sample.row["host subject id"] == "SD01696"
        assert sample.row["geographic location (country and/or sea)"] == "Italy"
        assert sample.row["geographic location (latitude)"] == "45.84"
        assert sample.row["collection date"] == "2021-05-19"

    def test_the_project_is_the_study_the_sample_goes_under(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        assert sample.row["project name"] == "PRJEB76898"

    def test_no_scientific_name_is_sent_for_enas_taxonomy_to_give(self, settings):
        """The laboratory's table names the host where ENA wants the name of
        the taxon the sample is registered as, a metagenome."""
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        assert "scientific_name" not in sample.row and sample.row["taxon_id"] == "1861841"

    def test_the_host_life_stage_goes_out_under_the_checklist_name(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        assert sample.row["host life stage"] == "adult"
        assert "host lifestage" not in sample.row

    def test_the_experiment_names_its_study_and_sample_by_alias(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        row = sample.experiments["EHI00032"]
        assert row["alias"] == "ena_EHI00032"
        assert row["study_alias"] == "5de02048-cb9b-4219-8380-adc0829dbbae"
        assert row["sample_alias"] == "AGP08"
        assert (row["library_strategy"], row["library_selection"], row["library_layout"], row["insert_size"]) == (
            "WGS", "RANDOM", "PAIRED", "400")
        assert (row["design_description"], row["library_source"]) == ("metagenome", "METAGENOMIC")

    def test_a_sample_ehi_core_knows_at_ena_is_joined_not_registered_again(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08", sample_ena_accessions=["ERS22455981", "ERS1"])],
                        settings)
        [sample] = found.samples
        assert (sample.accession, sample.alias, sample.row) == ("ERS22455981", "AGP08", None)
        assert sample.experiments["EHI00032"]["sample_alias"] == "AGP08"

    def test_two_libraries_of_one_new_sample_share_it(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08"), hologenome("EHI00033", "AGP08")], settings)
        [sample] = found.samples
        assert [e["code"] for e in sample.hologenomes] == ["EHI00032", "EHI00033"]

    def test_a_hologenome_already_deposited_is_skipped(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08", ena_run_accession="ERR13998795")], settings)
        assert found.samples == [] and found.skipped == ["EHI00032: already deposited as ERR13998795"]

    def test_a_sample_the_laboratory_has_not_described_is_reported_before_any_download(self, settings):
        entry = hologenome("EHI00032", "AGP08")
        entry["sample"].update(latitude=None, collection_date=None)
        found = plan_of([entry], settings)
        assert found.samples == []
        assert found.problems == ["EHI00032 (sample AGP08): the sample holds no collection date, "
                                  "geographic location (latitude) (fill it in Airtable, then sync)"]

    def test_a_sample_ehi_core_does_not_hold(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP99", sample=None)], settings)
        assert found.problems == ["EHI00032 (sample AGP99): ehi-core holds no such sample: "
                                  "sync the samples from Airtable"]

    def test_a_library_without_ena_words_for_its_instrument_is_reported(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08", instrument_model=None, platform="")], settings)
        assert found.problems == ["EHI00032: ehi-core holds no platform or instrument model for it"]

    def test_a_hologenome_without_reads_or_sample_code(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08", raw_reverse_url=None),
                         hologenome("EHI00033", "")], settings)
        assert found.problems == ["EHI00032: no raw forward and reverse reads",
                                  "EHI00033: no lab sample code, so ENA cannot be told about its sample"]

    def test_a_hash_would_cut_ena_upload_clis_row_off(self, settings):
        entry = hologenome("EHI00032", "AGP08")
        entry["sample"]["description"] = "Sample #3"
        found = plan_of([entry], settings)
        assert "'#' in sample_description" in found.problems[0]


# ---------------------------------------------------------------------------
# Depositing
# ---------------------------------------------------------------------------

class TestDeposit:
    def test_a_new_sample_is_registered_with_its_experiment_and_run(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        cli_ = FakeUploadCli(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                     run=("ena_EHI00032", "ERR1")))
        done = ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=cli_, fetch=fetch)

        assert done == ena.Deposited("EHI00032", "ERS1", "ERX1", "ERR1", "AGP08")
        assert sample.accession == "ERS1"
        assert cli_.tables() == ["sample", "experiment", "run"]
        cmd, folder = cli_.calls[0]
        assert cmd[1:3] == ["-m", "ena_upload.ena_upload"]
        assert "--checklist" in cmd and "ERC000013" in cmd and "--dev" not in cmd
        assert [Path(p).name for p in cmd[cmd.index("--data") + 1:]] == [
            "EHI00032_raw_1.fq.gz", "EHI00032_raw_2.fq.gz"]
        run_rows = (folder / "run.tsv").read_text().splitlines()
        assert run_rows[0] == "alias\texperiment_alias\tfile_name\tfile_type\tfile_checksum"
        assert run_rows[1] == f"ena_EHI00032\tena_EHI00032\tEHI00032_raw_1.fq.gz\tfastq\t{'0' * 32}"
        # The reads go once ENA holds them; the tables and the receipt stay.
        assert not (folder / "EHI00032_raw_1.fq.gz").exists()
        assert (folder / "receipt.xml").exists() and (folder / "ena-upload-cli.log").exists()

    def test_the_test_server_is_asked_for(self, settings):
        settings.test = True
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        cli_ = FakeUploadCli(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                     run=("ena_EHI00032", "ERR1")))
        ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=cli_, fetch=fetch)
        assert "--dev" in cli_.calls[0][0]

    def test_a_sample_ena_already_holds_is_taken_and_the_rest_sent_again(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        refused = receipt(False, errors=[
            'In sample, alias: "AGP08". The object being added already exists in the submission '
            'account with accession: "ERS22455981".'])
        cli_ = FakeUploadCli(refused, receipt(experiment=("ena_EHI00032", "ERX1"), run=("ena_EHI00032", "ERR1")))
        done = ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=cli_, fetch=fetch)

        assert done == ena.Deposited("EHI00032", "ERS22455981", "ERX1", "ERR1", "AGP08")
        assert cli_.tables(0) == ["sample", "experiment", "run"]
        assert cli_.tables(1) == ["experiment", "run"]

    def test_a_hologenome_ena_holds_whole_is_not_sent_or_downloaded_again(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08", sample_ena_accessions=["ERS22455981"])],
                           settings).samples
        already = 'In {kind}, alias: "ena_EHI00032". The object being added already exists in the ' \
                  'submission account with accession: "{acc}".'
        cli_ = FakeUploadCli(receipt(False, errors=[already.format(kind="experiment", acc="ERX1"),
                                                     already.format(kind="run", acc="ERR1")]))
        fetched = []
        done = ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=cli_,
                           fetch=lambda *a: fetched.append(a) or fetch(*a))
        assert done == ena.Deposited("EHI00032", "ERS22455981", "ERX1", "ERR1", "AGP08")
        assert len(cli_.calls) == 1 and len(fetched) == 2

    def test_a_refusal_is_reported_with_enas_reasons(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        cli_ = FakeUploadCli(receipt(False, errors=["Invalid value for collection date."]))
        with pytest.raises(ena.EnaError, match="collection date"):
            ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=cli_, fetch=fetch)
        assert sample.accession is None

    def test_a_run_that_submitted_nothing_shows_what_ena_upload_cli_said(self, settings):
        [sample] = plan_of([hologenome("EHI00032", "AGP08")], settings).samples
        with pytest.raises(ena.EnaError, match="(?s)submitted nothing.*ena-upload-cli output"):
            ena.deposit(sample.hologenomes[0], sample, settings, CREDS, run=FakeUploadCli(""), fetch=fetch)

    def test_the_second_library_of_a_new_sample_joins_the_sample_the_first_registered(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08"), hologenome("EHI00033", "AGP08")], settings)
        cli_ = FakeUploadCli(
            receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"), run=("ena_EHI00032", "ERR1")),
            receipt(experiment=("ena_EHI00033", "ERX2"), run=("ena_EHI00033", "ERR2")),
        )
        kept = []
        outcome = ena.deposit_all(found, settings, CREDS, on_deposit=kept.append, run=cli_, fetch=fetch)

        assert [d.code for d in outcome.deposited] == ["EHI00032", "EHI00033"]
        assert [d.sample for d in kept] == ["ERS1", "ERS1"]
        assert cli_.tables(1) == ["experiment", "run"]
        assert (settings.work_dir / "EHI00033" / "experiment.tsv").read_text().splitlines()[1].split("\t")[3] == "AGP08"

    def test_one_hologenome_refused_does_not_stop_the_others(self, settings):
        found = plan_of([hologenome("EHI00032", "AGP08"), hologenome("EHI00034", "AGP09")], settings)
        answers = {
            "EHI00032": receipt(False, errors=["Invalid value for collection date."]),
            "EHI00034": receipt(sample=("AGP09", "ERS2"), experiment=("ena_EHI00034", "ERX2"),
                                run=("ena_EHI00034", "ERR2")),
        }

        def run(cmd, folder, creds):
            (folder / "receipt.xml").write_text(answers[folder.name])
            return subprocess.CompletedProcess(cmd, 0, "", "")

        settings.parallel = 2
        outcome = ena.deposit_all(found, settings, CREDS, run=run, fetch=fetch)
        assert [d.code for d in outcome.deposited] == ["EHI00034"]
        assert len(outcome.failed) == 1 and outcome.failed[0].startswith("EHI00032: ENA refused")


def test_fetch_reads_takes_the_checksum_as_the_file_arrives(tmp_path):
    source = tmp_path / "reads.fq.gz"
    source.write_bytes(b"ACGT" * 1000)

    class Response:
        def __init__(self):
            self._data = [source.read_bytes(), b""]

        def read(self, size):
            return self._data.pop(0)

        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

    dest = tmp_path / "work" / "EHI00032_raw_1.fq.gz"
    with patch("urllib.request.urlopen", return_value=Response()):
        md5 = ena.fetch_reads("https://erda/RAW/EHI00032_1.fq.gz", dest, 10)
    assert md5 == ena.md5_of(source)
    assert dest.read_bytes() == source.read_bytes()
    assert not dest.with_name(dest.name + ".part").exists()
    # A file already there is kept, and its checksum read from it.
    with patch("urllib.request.urlopen", side_effect=AssertionError("not downloaded again")):
        assert ena.fetch_reads("https://erda/RAW/EHI00032_1.fq.gz", dest, 10) == md5


# ---------------------------------------------------------------------------
# ehio ena
# ---------------------------------------------------------------------------

CONFIG = {"ENA_OUTPUT_BASE": "", "ENA_SECRET_FILE": ""}


def ena_args(tmp_path, **extra) -> argparse.Namespace:
    values = dict(batch="EHS0001", work_dir=str(tmp_path / "EHS0001"), test=False, dry_run=False,
                  keep_reads=False, parallel=None, download_timeout=600.0, verbose=False, core_token=None)
    values.update(extra)
    return argparse.Namespace(**values)


def submission(*entries, study="PRJEB76898") -> FakeCoreClient:
    return FakeCoreClient(batches={"EHS0001": {
        "row": {"code": "EHS0001", "status": "Running", "study_accession": study},
        "entries": list(entries),
    }})


def run_ena(tmp_path, fake, upload_cli, monkeypatch, **extra) -> int:
    monkeypatch.setenv("ENA_USERNAME", "Webin-1")
    monkeypatch.setenv("ENA_PASSWORD", "pw")
    served = {"PRJEB76898": STUDY_XML, "ERS22455981": SAMPLE_XML}
    with patch.object(cli.cfg, "get", side_effect=lambda k, d=None: CONFIG.get(k, d)), \
         patch.object(cli, "_resolve_token", side_effect=AssertionError("Airtable is not needed")), \
         patch("ehio.airtable.AirtableClient", side_effect=AssertionError("Airtable is not read")), \
         patch.object(ena, "_fetch", side_effect=lambda url, creds, timeout: next(
             (body for acc, body in served.items() if url.endswith(acc)), None)), \
         patch.object(ena, "run_upload_cli", upload_cli), \
         patch.object(ena, "fetch_reads", fetch), \
         using(fake):
        return cli.cmd_ena(ena_args(tmp_path, **extra))


class TestCommand:
    def test_the_accessions_are_written_onto_each_hologenome_and_the_batch_is_done(self, tmp_path, monkeypatch):
        fake = submission(hologenome("EHI00032", "AGP08"),
                          hologenome("EHI00031", "AGP08", ena_run_accession="ERR13998795"))
        upload = FakeUploadCli(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                       run=("ena_EHI00032", "ERR1")))
        assert run_ena(tmp_path, fake, upload, monkeypatch) == 0

        [written] = fake.rows("hologenomes")
        assert written["key"] == {"code": "EHI00032"}
        assert written["values"] == {"ena_study_accession": "PRJEB76898", "ena_sample_accession": "ERS1",
                                     "ena_experiment_accession": "ERX1", "ena_run_accession": "ERR1"}
        # The lab sample holds its ENA sample too, which its later libraries join.
        [sample] = fake.rows("samples")
        assert (sample["key"], sample["values"]) == ({"code": "AGP08"}, {"ena_sample_accession": "ERS1"})
        # Both in one request, so neither is written without the other.
        assert [table for table, _ in fake.upserts[0]] == ["samples", "hologenomes"]
        [batch] = fake.rows("ena_submissions")
        assert batch["values"]["status"] == "Done"
        assert "Deposited 1 hologenome(s): EHI00032 (ERR1)" in batch["values"]["log"]
        assert "Already deposited: 1." in batch["values"]["log"]
        assert batch["defaults"]["run_on"]
        assert (tmp_path / "EHS0001" / "EHS0001_accessions.tsv").read_text().splitlines()[1] == \
            "EHI00032\tERS1\tERX1\tERR1"

    def test_what_was_not_deposited_leaves_the_batch_in_error_with_the_reasons(self, tmp_path, monkeypatch):
        fake = submission(hologenome("EHI00032", "AGP08"), hologenome("EHI00033", "AGP99", sample=None))
        upload = FakeUploadCli(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                       run=("ena_EHI00032", "ERR1")))
        assert run_ena(tmp_path, fake, upload, monkeypatch) == 1
        assert [row["key"]["code"] for row in fake.rows("hologenomes")] == ["EHI00032"]
        [batch] = fake.rows("ena_submissions")
        assert batch["values"]["status"] == "Error"
        assert "EHI00033 (sample AGP99): ehi-core holds no such sample" in batch["values"]["log"]

    def test_a_test_submission_keeps_no_accession_and_leaves_the_status_alone(self, tmp_path, monkeypatch):
        fake = submission(hologenome("EHI00032", "AGP08"))
        upload = FakeUploadCli(receipt(sample=("AGP08", "ERS1"), experiment=("ena_EHI00032", "ERX1"),
                                       run=("ena_EHI00032", "ERR1")))
        assert run_ena(tmp_path, fake, upload, monkeypatch, test=True) == 0
        assert "--dev" in upload.calls[0][0]
        assert fake.rows("hologenomes") == [] and fake.rows("samples") == []
        [batch] = fake.rows("ena_submissions")
        assert set(batch["values"]) == {"log"} and "TEST" in batch["values"]["log"]

    def test_a_dry_run_writes_the_tables_and_sends_nothing(self, tmp_path, monkeypatch):
        fake = submission(hologenome("EHI00032", "AGP08"))
        upload = FakeUploadCli()
        assert run_ena(tmp_path, fake, upload, monkeypatch, dry_run=True) == 0
        assert upload.calls == [] and fake.upserts == []
        folder = tmp_path / "EHS0001" / "EHI00032"
        assert sorted(p.name for p in folder.iterdir()) == ["experiment.tsv", "run.tsv", "sample.tsv"]

    def test_a_submission_without_a_study_is_refused(self, tmp_path, monkeypatch):
        fake = submission(hologenome("EHI00032", "AGP08"), study="")
        with pytest.raises(SystemExit):
            run_ena(tmp_path, fake, FakeUploadCli(), monkeypatch)
        assert fake.upserts == []

    def test_a_submission_ehi_core_does_not_hold(self, tmp_path, monkeypatch, capsys):
        with pytest.raises(SystemExit):
            run_ena(tmp_path, FakeCoreClient(), FakeUploadCli(), monkeypatch)
        assert "holds no ENA submission 'EHS0001'" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Scanning and statuses
# ---------------------------------------------------------------------------

class TestScanning:
    def test_ena_is_scanned_like_the_other_modules(self):
        assert "ena" in MODULES

    def test_its_batches_are_looked_for_in_ehi_core_alone(self):
        core = MagicMock()
        [source] = _sources("ena", token="tok", core=core)
        assert source.name == "ehi-core" and source.table == "ena_submissions"

    def test_the_launch_script_runs_ehio_ena_and_no_drakkar(self):
        script = build_script_content("ena", "EHS0001", "/RUN/EHS0001", "/ENA/EHS0001", "slurm")
        assert "ehio ena -b EHS0001 -d /ENA/EHS0001\n_EHIO_SUCCESS=1\n" in script
        assert "drakkar ena" not in script and "_ehio_drakkar_start\n" not in script.split("cd /ENA/EHS0001")[1]
        assert "ehio set-status --module ena --batch EHS0001 --status Error" in script


def test_set_status_writes_an_ena_submission_to_ehi_core_alone():
    fake = FakeCoreClient()
    args = argparse.Namespace(module="ena", batch="EHS0001", status="Error", failures_dir="/ENA/EHS0001",
                              failures_since="0", airtable_token=None, core_token=None)
    with using(fake), patch.object(cli, "_resolve_token", side_effect=AssertionError("no Airtable")):
        assert cli.cmd_set_status(args) == 0
    [row] = fake.rows("ena_submissions")
    assert (row["key"], row["values"]) == ({"code": "EHS0001"}, {"status": "Error"})
