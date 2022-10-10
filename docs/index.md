---
hide:
  - navigation
---

# BrickFlow

BrickFlow is a CLI tool for development and deployment of Python based Databricks Workflows in a declarative way.

## Concept

`brickflow` aims to improve development experience for building any pipelines on databricks via:

- Providing a declarative way to describe workflows via decorators
- Provide intelligent defaults to compute targets
- Provide a code and git first approach to managing and deploying workflows
- Use IAC such as terraform to manage the state and deploy jobs and their infrastructure.
- CLI tool helps facilitate setting up a projects
- Provides additional functionality through the context library to be able to do additional things for workflows.

## Limitations & Missing Features in the Project

- [ ] Docs (WIP)
- [x] Notebook Tasks
- [ ] Serverless Sql Tasks
- [ ] DLT Pipeline Tasks
- [x] Support for Job Clusters
- [ ] Support for Look Up Clusters (given a cluster name auto fetch the id)
- [ ] Support for Warehouses
- [ ] Support for using existing warehouses
- [ ] CLI for initializing repo to setup entry points and required files
- [ ] CLI for visualizing workflow locally using a graphing tool

## Legal Information

!!! danger "Support notice"

    This software is provided as-is and is not officially supported by
    Sriharsha Tikkireddy nor Databricks through customer technical support channels. Support, questions, and feature requests can be communicated through the Issues
    page of the [repo](https://github.com/stikkireddy/brickflow/issues). Please understand that
    issues with the use of this code will not be answered or investigated by
    Databricks Support.

## Feedback

Issues with `brickflow`? Found a :octicons-bug-24: bug?
Have a great idea for an addition? Want to improve the documentation? Please feel
free to file an [issue](https://github.com/stikkireddy/brickflow/issues/new).

## Contributing

To contribute please fork and create a pull request.
