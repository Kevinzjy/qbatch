# qbatch

A simplified version of workflow management system for scalable data analysis.

# Change log

- Version 0.2.0: move sample_sheet from header to job section, add group_sheet support

# Usage

# Installation

`qbatch` is heavily inspired by Snakemake, but uses native SLURM/Torque dependencies to build analysis pipeline.

`qbatch` runs in **python3.7+** with the `tomli` package installed, no other dependencies are required.

```bash
pip install tomli
```

# Tutorial: Setting up the TOML pipeline configuration

## A quick example

The main input is a configuration in TOML format. Here's a simple example:

```toml
# Parameters
pipeline = "kallisto_lncipedia"
version = "2023.5.2"
work_dir = "/histor/zhao/zhangjy/test_data/kallisto_lncipedia"
pbs_scheduler = "torque"
pbs_server = 'mu02'
pbs_queue = 'batch'
pbs_walltime = 1200

# Reference
index = "/histor/public/database/LNCipedia/merged_lnc52hc_gencodev29.annotated.fa.idx"

# Executable
kallisto = "/histor/public/software/kallisto/kallisto"

# Rules
[job.kallisto]
sample_sheet = "/histor/zhao/zhangjy/test_data/sample_sheet.csv"
input = [
    {r1 = "{sample.read1}"},
    {r2 = "{sample.read2}"},
]
out_dir = "{work_dir}/out_kallisto"
output = "{out_dir}/{sample.name}/abundance.h5"
threads = 32
log = "{work_dir}/logs/{sample.name}.kallisto.logs"
shell = """
mkdir -p {out_dir}
{kallisto} quant -t {threads} -b 100 -i {index} -o {out_dir}/{sample.name} {input.r1} {input.r2}
"""
```

See to `example/Hi-C.toml` and `example/samples.csv` for another example TOML and sample sheet configuration.

## The header section of the TOML file

In the header section, the pipeline/version/work_dir/sample_sheet fields is required to specify the pipeline information:

| field | description |
|------|-------------|
| pipeline | name of the pipeline |
| version | version of the pipeline |
| work_dir | path to the main output directory of the pipeline, will be created automatically if it does not exist |

Supported variables for submitting PBS jobs are:

| field | description |
|------|-------------|
| pbs_scheduler | name of the scheduler, should be either 'torque' or 'slurm' |
| pbs_server | name of the submitting host |
| pbs_queue | queue for submitting jobs |
| pbs_walltime | number of requested maximum hours each job can run |
| threads | default threads for PBS jobs |

Other variables defined in the header section (e.g. `kallisto` and `index`) can also be accessed globally.

**NOTE**: only `str` and `int` variables are allowed in the header section.

## Job configuration

The name of each job should be strictly in `[job.job_name]` format (e.g. `[job.kallisto]`, `[job.sleuth]`).

**The only mandatory job field is `shell`**, but some variables can also be defined for each job separately:

| Variable | Type | Description | Default |
|-|-|-|-|
| depend |  str / list | name of job dependencies | No dependencies |
| sample_sheet | str | path to sample_sheet file | Optional |
| group_sheet | str | path to group_sheet file | Optional |
| input | str / list of dicts | version of the pipeline | No specific input files |
| output | str / list of dicts | path to the main output directory of the pipeline, will be created automatically if it does not exist | No specific output files |
| threads | int | numer of cpu cores | same as threads in header section |
| gpus | int | number of gpus | 0 |
| queue | str | name of queue | same as pbs_queue |
| walltime | int | number of maximum hours to run | same as pbs_walltime |
| dir| str | path to workdir | same as work_dir |
| log | str | path to log file | {work_dir}/logs/{job}.logs |
| **shell** | str | shell script to execute | required |

All variables defined in each job can be accessed locally using `{var_name}`, e.g. `{threads}` and `{dir}`.

The priority of variables: sample > group > job > global

### The sample sheet

The sample_sheet is a comma separated file, where the first row is the name of each column, and the other rows represent the samples to be analysed.
The only required column is `name`, which should be used to define the name of each sample. Other columns can be named as you wish.

```
name,read1,read2
26-ADJ,/data/26-ADJ_1.fq.gz,/data/26-ADJ_2.fq.gz
```

Any columns defined in the `sample_sheet.csv` can be accessed in the TOML file using `{sample.name}` or `{sample.col_name}`.

If sample_sheet is defined, a copy of job for each sample will be submitted.

### The group sheet

The group_sheet should be in the same format as sample sheet file, but should be accessed using `{group.name}` and `{group.col_name}'.

If both sample_sheet and group_sheet are defined, a copy of job for each sample and group combination will be submitted.

### Job dependencies

The `depend` field is the name / list of names of job dependencies. For example,

```toml
# Single dependency, job will start when [job.hisat2] for the same sample has finished successfully.
depend = "hisat2"

# Multiple dependency, job will only start if both [job.hisat2] and [job.stringtie] has finished successfully.
depend = ["hisat2", "stringtie"]
```

### Input / output files

The `input` and `output` fields can be in str or dict list format.

For example,

```toml
# Single input, defined value can be accessed with {input} or {output}
input = "path/to/{sample.name}.fastq.gz"
output = "path/to/{sample.name}.sorted.bam"

# Multiple input, defined values can be accessed with {input.read1} / {input.read2} / {output.bam} / {output.unmapped}.
input = [
    {read1 = "path/to/{sample.name}_r1.fastq.gz"},
    {read2 = "path/to/{sample.name}_r2.fastq.gz"},
]
output = [
    {bam = "path/to/{sample.name}.sorted.bam"},
    {unmapped = "path/to/{sample.name}.unmapped.fastq"}
]
```

The `output` field is used to determine whether a job has been finished. If all files/directories exist, the job will not be committed.

### Resource Limits

This section contains some variables for controlling the resources requested in SLURM/Torque system.

- The `threads` controls the number of requested cpu cores for each job.
- The `gpus` controls the number of requested gpu cards for each job.
- The `queue` controls the queue for submitting each job.
- The `walltime` controls the maximum requested run time (hours) for each job.
- The `dir` controls the working directory of each job.
- The `log` controls the stdout/stderr of each job. The stdout and stderr are combined into a single file.

### Command Lines

The `shell` field is the command line for each job. This is a mandatory field, and should be provided in multiple lines as defined by `""" """`.

For example:

```toml
shell = """
mkdir -p {out_dir}
source path/to/some/scripts
{kallisto} quant -t {threads} -b 100 -i {index} -o {out_dir}/{sample.name} {input.r1} {input.r2}
"""
```

# Usage

## Submit a pipeline

For each sample, `qbatch` will submit all jobs in the defined order, and dependencies will be added if the `depend` field is defined.

```bash
qbatch your_pipeline.toml
```

After a successful commit, the following directory will be created:

```
├── checkpoint              # IDs of successfully completed jobs
├── info                    # JSON formatted pipeline information
├── pipeline                # a copy of the submitted TOML configuration
├── jobs                    # A tab separated files of job submission record file with 3 columns: name/job_id/pbs_command
├── logs                    # directory for log files
│   ├── sample1.job1.logs
│   ├── sample1.job2.logs
│   ├── sample2.job1.logs
│   └── sample2.job2.logs
└── sc                      # directory for PBS scripts
    ├── job1.sample1.sh
    ├── job2.sample1.sh
    ├── job1.sample2.sh
    └── job2.sample2.sh
```

## Cancelling a pipeline

If the dependencies of a job have failed to complete. The job is canceled in the ***Torque*** scheduler. However, the ***SLURM*** jobs will be stuck and marked as dependencies never satisfied. So, a simple `qcancel` command is provided to cancel all unfinished jobs in a pipeline:

```bash
qcancel work_dir/jobs
```

## Continuing a pipeline

You can also continue unfinished jobs using the record pipeline file. Only jobs recorded in `jobs` and not presented in `checkpoint` will be resubmitted.

```bash
qbatch -c work_dir/pipeline
```

## Testing a pipeline

You can use `-d` option to specify a dry run, where jobs will be parsed but not submitted to the PBS server

```bash
qbatch -d pipeline.toml
```

# **NOTE**

- All paths specified in the TOML and sample_sheet / group_sheet files should be absolute paths.
- The values defined in `{var}` are parsed line by line. So the variable should be defined in the preceding lines.


