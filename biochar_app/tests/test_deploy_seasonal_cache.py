"""Guard ordering and resource isolation in the production publication script."""
from pathlib import Path


def test_deploy_prepares_transfers_and_verifies_before_start():
    script = (Path(__file__).resolve().parents[2] / "deploy.sh").read_text()
    main = script.split("# Main\n", 1)[1]
    steps = ["prepare_local_cache", "begin_remote_maintenance", "sync_data",
             "verify_remote_cache", "restart_remote_service"]
    assert [main.index(step) for step in steps] == sorted(main.index(step) for step in steps)
    sync = script.split("sync_data() {", 1)[1].split("prepare_local_cache()", 1)[0]
    assert '"${LOCAL_CACHE_DIR}/"' in sync
    assert '"${REMOTE_HOST}:${REMOTE_CACHE_DIR}/"' in sync
    remote_check = script.split("verify_remote_cache() {", 1)[1].split("restart_remote_service()", 1)[0]
    assert "gseason_cache_warmer --all-years --verify-only" in remote_check
    assert "gseason_cache_warmer" not in script.split("restart_remote_service() {", 1)[1].split("print_post_checks()", 1)[0]
