import json
import os


def test_metrics_order(tmp_dir, dvc):
    tmp_dir.gen(
        {
            "p.json": json.dumps({"p1": 1}),
            "p1.json": json.dumps({"p2": 1}),
            "sub": {
                "p3.json": json.dumps({"p3": 1}),
                "p4.json": json.dumps({"p4": 1}),
            },
        }
    )

    dvc.stage.add(
        metrics=["p.json", str(tmp_dir / "sub" / "p4.json")],
        cmd="cmd1",
        name="stage1",
    )
    with (tmp_dir / "sub").chdir():
        dvc.stage.add(
            metrics=[str(tmp_dir / "p1.json"), str("p3.json")],
            cmd="cmd2",
            name="stage2",
        )

    assert list(dvc.metrics.show()[""]) == [
        "p.json",
        os.path.join("sub", "p4.json"),
        "p1.json",
        os.path.join("sub", "p3.json"),
    ]
