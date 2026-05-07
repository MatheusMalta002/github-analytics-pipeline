from dagster import instance_for_test
from orchestration.definitions import defs


def main():
    job = defs.resolve_job_def("github_pipeline_job")
    with instance_for_test() as instance:
        result = job.execute_in_process(instance=instance)
        if not result.success:
            raise Exception("Pipeline falhou")
        print("Pipeline executado com sucesso")


if __name__ == "__main__":
    main()