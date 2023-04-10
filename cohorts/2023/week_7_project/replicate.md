# Project Setup

## Prerequisites:

<details>
<summary>Python 3</summary>

This project was tested with Python 3.11. Use a [Python version manager](https://realpython.com/intro-to-pyenv/) and a [virtual environment](https://realpython.com/python-virtual-environments-a-primer/) to install your dependencies.

</details>

<details>
<summary>Google Cloud Platform Account</summary>

Sign up for a free test account [here](https://cloud.google.com/free/), and enable billing.

</details>

<details>
<summary>Prefect Cloud</summary>

Sign up for a free account [here](https://www.prefect.io).

</details>

<details>
<summary>Google Cloud CLI</summary>

Installation instruction for `gcloud` [here](https://cloud.google.com/sdk/docs/install-sdk).

</details>

<details>
<summary>dbt Cloud</summary>

Sign up for a free account [here](https://www.getdbt.com/signup/).

</details>

<details>
<summary>Rapid API</summary>

Sign up for a free account [here](https://rapidapi.com/auth/sign-up/).

</details>

<details>
<summary>Terraform</summary>

You can view the [installation instructions for Terraform here](https://developer.hashicorp.com/terraform/downloads?ajs_aid=f70c2019-1bdc-45f4-85aa-cdd585d465b4&product_intent=terraform)

</details>

<details>
<summary>Git and Github Repository</summary>

To install git, check out instructions [here](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git).
Creation steps for a [remote github repository here](https://docs.github.com/en/get-started/quickstart/create-a-repo).

</details>

</br>

## Setup Steps:

1. Clone this repository using this [link](https://minhaskamal.github.io/DownGit/#/home?url=https://github.com/Joseun/data-engineering-zoomcamp/tree/main/cohorts/2023/week_7_project)
1. Remove git history with `rm -rf .git`
1. Rename the `env` file to `.env`, and replace the Rapid API Key after subscription
1. Reinitialize git with `git init`</br>
1. Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
1. Configure Identity and Access Management (IAM) for the service account, giving it the following privileges: BigQuery Admin, Storage Admin and Storage Object Admin
1. Download the JSON credentials and save it, e.g. to `~/.gc/<credentials>`
1. Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
1. Let the [environment variable point to your GCP key](https://cloud.google.com/docs/authentication/application-default-credentials#GAC), authenticate it and refresh the session token
   ```
   export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
   gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
   gcloud auth application-default login
   ```
1. Create a virtual environment `python -m venv venv` and activate it with `source venv/bin/activate`
1. Install all required dependencies into your environment
   ```
   pip install -r requirements.txt
   ```
1. Assuming you are using Linux AMD64 run the following commands to install Terraform - if you are using a different OS please choose the correct version [here](https://developer.hashicorp.com/terraform/downloads) and exchange the download link and zip file name

   ```
   sudo apt-get install unzip
   cd ~/bin
   wget https://releases.hashicorp.com/terraform/1.4.1/terraform_1.4.1_linux_amd64.zip
   unzip terraform_1.4.1_linux_amd64.zip
   rm terraform_1.4.1_linux_amd64.zip
   ```
1. Go into your terraform directory `cd terraform`
1. Run `terraform init` to initialize.
1. Run `terraform plan` to see the changes to be applied.
1. Run `terraform apply` to deploy your resources.
1. Run the following commands to set up a local Prefect profile
   ```
    prefect profile create nbateams
    prefect profile use nbateams
    prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

1. Open two new terminal windows and ssh into your VM.  These additional terminals are going to be for launching the Prefect orion server, and to launch a work queue, which will process deployed pipelines

    * Additional window 1:
      ```
        prefect orion start
      ```
    * Additional window 2:
      ```
      prefect agent start --work-queue "default"
      ```

1. You can set up [prefect blocks](https://docs.prefect.io/latest/concepts/blocks/) from Prefect UI

1. From your original terminal session, run the following three commands to deploy the pipeline to Prefect and then run it for all years of data
   ```
    prefect deployment build flows/players_gcs.py:etl_players_flow -n "Rapid-API-to-GCS"
    prefect deployment apply etl_players_flow-deployment.yaml
    prefect deployment run etl_players_flow/Rapid-API-to-GCS -p "year=2022"
   ```

    This may take 15 minutes to run the full pipeline as API subscription limits the number of calls in a minute. You can switch to the terminal session for the work queue to watch the progress if you like.

1. In the `nba_teams/models`, make sure that the `schema.yml` file matches up with your BigQuery setup. Also in `stg_{nickname}_players.sql`, make sure that the `source` reference matches up the name of your BigQuery table. Also check that all the references to db and tables in the `core` folder are ok.
1. Push the code to your own remote repository.
   ```
   git add .
   git commit -m 'initial commit'
   git remote add origin url-of-your-git-repo
   git branch -M main
   git push -u origin main
   ```
1. > **Important note: Once you're done evaluating this project, make sure to stop and remove any cloud resources.  If you're using a cloud VM, make sure to stop it in your VM Instances screen in Google Cloud Console, and potentially delete it if you no longer want it.  This way it's not up and running, and using up your credits.  In addition, you can use Terraform to destroy your buckets and datasets, with**
   ```
   terraform -chdir="./terraform" destroy -var="project=<project id here>"
   ```

</br>
</br>
