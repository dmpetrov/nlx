import json
import os


def test_plots_order(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "p.json": json.dumps([{"p1": 1}, {"p1": 2}]),
            "p1.json": json.dumps([{"p2": 1}, {"p2": 2}]),
            "sub": {
                "p3.json": json.dumps([{"p3": 1}, {"p3": 2}]),
                "p4.json": json.dumps([{"p4": 1}, {"p4": 2}]),
            },
        }
    )

    dvc.stage.add(
        plots=["p.json", str(tmp_dir / "sub" / "p4.json")],
        cmd="cmd1",
        name="stage1",
    )
    with (tmp_dir / "sub").chdir():
        dvc.stage.add(
            plots=[str(tmp_dir / "p1.json"), "p3.json"],
            cmd="cmd2",
            name="stage2",
        )

    assert list(dvc.plots.show()) == [
        "p.json",
        os.path.join("sub", "p4.json"),
        "p1.json",
        os.path.join("sub", "p3.json"),
    ]
