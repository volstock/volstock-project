# Automated Actions on this Repo

- N.B This applies to all branches in this repo

- There are two workflows. 

## Run Tests Workflow

- The workflow checks out the repo, sets up Python and Venv, installs requirements and Dev Setup and then runs several tests 
- Firstly, the unit tests you have written be run automatically. If they fail, deployment will stop. 
- Secondly, Flake8 will check your code against the PEP8 Python Standard. If there are any issues, deployment will stop. 
- And so on with Black (a code reformatter), Bandit (a package security analyser) and coverage (a test coverage checker e.g what percentage of source code has a test associated with it). These tests will not prevent deployment in our experience so far, but will provide warnings.

## Terraform Workflow
- This is a Work in Progress as the CICD pair have not got access to the production TF Config File.
- This workflow automates the Infrastructure as Code Process. Your code will be deployed to AWS when you push your code to GitHub.