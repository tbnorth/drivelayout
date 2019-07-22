import file_db
import light_orm as lo

import pytest
from mkfakefs import makefilehier

from collections import namedtuple
from addict import Dict

FakeFS = namedtuple("FakeFS", "path db")

GOLD = Dict(n=342, dupe_pairs=23)


@pytest.fixture
def fakefs(tmp_path_factory):
    base = tmp_path_factory.mktemp("tmp")
    makefilehier(base)
    return FakeFS(
        path=str(base),
        db=str(tmp_path_factory.mktemp("tmp").joinpath("tmp.db")),
    )


def test_create_db(fakefs):
    "just a weak end to end test for now" ""

    opt = ['--db', fakefs.db, '--path', fakefs.path]
    file_db.run_opt(file_db.get_options(opt))
    con, cur = lo.get_con_cur(fakefs.db)
    count = lo.do_one(cur, "select count(*) as n from file")
    assert count.n == GOLD.n
    opt += ['--update-hashes', '--dupes-only']
    file_db.run_opt(file_db.get_options(opt))
    count = lo.do_one(
        cur, "select count(*) as n from file where hash is not null"
    )
    assert count.n == GOLD.dupe_pairs * 2
