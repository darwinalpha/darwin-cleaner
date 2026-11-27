"""规则值对象测试：分叉计划解析。"""

from darwin_cleaner import ForkPlan, RowForkSpec, Rules


def test_no_filters_no_outputs_yields_no_plan():
    assert Rules().to_fork_plans() == ()


def test_outputs_only_yields_single_all_plan():
    plans = Rules(outputs=("a", "b")).to_fork_plans()
    assert plans == (ForkPlan(key="all", output_names=("a", "b")),)


def test_filters_yield_plans_in_insertion_order():
    rules = Rules(
        filters={
            "stk": RowForkSpec(key="stk", field="code", regex="60", outputs=("a",)),
            "etf": RowForkSpec(key="etf", field="code", regex="51", outputs=("b", "c")),
            "empty": RowForkSpec(key="empty", outputs=()),
        }
    )
    plans = rules.to_fork_plans()
    assert [p.key for p in plans] == ["stk", "etf"]
    assert plans[1].output_names == ("b", "c")
    assert plans[0].row_filter.regex == "60"
