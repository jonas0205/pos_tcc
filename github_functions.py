# Databricks notebook source
import requests, base64, json, uuid
from typing import Optional

# COMMAND ----------


class GithubHandler:
    """This is a class for handling with github actions on notebooks.

    Attributes
    ----------
        token : str
            Returning Git Token secret to access BrewDat repositories.
        owner: str
            Returning owner of git hub projects

    """

    def __init__(self) -> None:
        akv = f"brewdatbeesghqsalesakv{ environment[0] }"

        self.token = dbutils.secrets.get(akv, "githubtok-brewdat-bees-repos-frwk-rw")
        self.owner = "BrewDat"
        self.repositories = [
            "brewdat-bees-sales-data-quality",
            "brewdat-bees-sales-repo-adb",
            "brewdat-sales-bees-commperf-adb",
            "brewdat-sales-bees-nectar-adb",
        ]

    # create docstring
    def get_git_reference_branch(
        self, repository_name: str, reference_branch: str = "develop"
    ):
        """This method wil get the sha hash of a reference_branch ( develop as default ) for a repository passed as method parameter.

        Args:
            repository_name (str): Repository where will look for a reference_branch
            reference_branch (str, optional): The reference branch that sha is wanted. Defaults to "develop".

        Returns:
            Json: Json with the status of the request, if success we will receive 200, if failure we will receive 400 and the stacktrace
        """

        if repository_name not in self.repositories:
            raise Exception(
                "This repository is not available.\n"
                f"Please choose one of the valid options on { self.repositories }:"
            )

        # Default parameters
        try:
            reference_branch = reference_branch.replace(" ", "_")

            general_url = f"https://api.github.com/repos/{ self.owner }/{ repository_name }/git/refs/heads/"

            repo_info_response = requests.get(
                general_url + reference_branch,
                headers={"Authorization": "Token " + self.token},
            )

            repo_info_response = repo_info_response.json()

            # TODO: create a else condition to raise an exception if we doesnt found the reference_branch in the response.
            if type(repo_info_response) == dict:
                if repo_info_response["ref"].split("/")[-1] == reference_branch:
                    branch_sha = repo_info_response["object"]["sha"]
                else:
                    raise Exception(
                        f" reference_branch { reference_branch } not found on the repository { repository_name }"
                    )

            elif type(repo_info_response) == list:
                # get reference branch hash
                for i in range(len(repo_info_response)):
                    if repo_info_response[i]["ref"].split("/")[-1] == reference_branch:
                        branch_sha = repo_info_response[i]["object"]["sha"]
                    else:
                        raise Exception(
                            f" reference_branch { reference_branch } not found on the repository { repository_name }"
                        )

            else:
                raise (
                    "Variable repo_info_response is with unsuported type! "
                    + type(repo_info_response)
                )

        except Exception as e:
            return {"status": 400, "Err": e, "response": repo_info_response}

        return {"status": 200, "response": repo_info_response, "reference": branch_sha}

    def create_git_branch(
        self, repository_name: str, branch_refence: str, new_branch_name: str = None
    ):
        """This method will create a new branch at the repository.

        Args:
            repository_name (str): Repository name we will create a new branch
            branch_refence (str): Branch reference that we will use as reference to create the new branch
            new_branch_name (str, optional): The new branch name. Defaults to None.

        Returns:
            Json: Json with the status of the request, if success we will receive 200, if failure we will receive 400 and the stacktrace
        """

        if repository_name not in self.repositories:
            raise Exception(
                "This repository is not available.\n"
                f"Please choose one of the valid options on { self.repositories }:"
            )

        try:
            # This function will fail only if we already have a branch with the same name.
            api_branch_url = f"https://api.github.com/repos/{ self.owner }/{ repository_name }/git/refs"

            new_branch_name = new_branch_name.replace(" ", "_")

            new_branch_response = requests.post(
                api_branch_url,
                json={"ref": f"refs/heads/{ new_branch_name }", "sha": branch_refence},
                headers={"Authorization": "Token " + self.token},
            )
        except Exception as e:
            return {"status": 400, "response": new_branch_response}

        return {"status": 200, "response": new_branch_response}

    def commit(
        self,
        repository_name: str,
        branch_name: str,
        path_file: str,
        file_name: str,
        content: str,
        message: Optional[str] = None,
    ):
        """This method will commit a content file based on a repository, branch, path and file name passed as method parameters.

        Args:
            repository_name (str): Repository name we will create a new branch.
            branch_name (str): Branch reference that we will use as reference to commit the content file.
            path_file (str): Path inside repository where content file will be commited.
            file_name (str): Name of notebook file that will be commited on the path above.
            content (str): The notebook content that will be commited.

        Returns:
            Json: Json with the status of the request, if success we will receive 200, if failure we will receive 400 and the stacktrace
        """

        if repository_name not in self.repositories:
            raise Exception(
                "This repository is not available.\n"
                f"Please choose one of the valid options on { self.repositories }:"
            )

        try:
            if not message:
                message = f"Commiting changes on { path_file }/{ file_name }."

            branch_name = branch_name.replace(" ", "_")

            # Verification of the existence of the file on the develop branch (branch where will be made the pull request off the commit).

            url_verification = f"https://api.github.com/repos/BrewDat/{ repository_name }/contents/{ path_file }/{ file_name }?ref=develop"
            response = requests.get(
                url_verification, headers={"Authorization": "Token " + self.token}
            ).json()

            flag_file_exists = "message" not in response.keys()

            if flag_file_exists:
                data = {
                    "message": message,
                    "content": base64.b64encode(content.encode("utf-8")).decode(
                        "ascii"
                    ),
                    "sha": response["sha"],
                    "branch": branch_name,
                }
            else:
                data = {
                    "message": message,
                    "content": base64.b64encode(content.encode("utf-8")).decode(
                        "ascii"
                    ),
                    "branch": branch_name,
                }

            url = f"https://api.github.com/repos/{ self.owner }/{ repository_name }/contents/{ path_file }/{ file_name }"

            commit_response = requests.put(
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer { self.token }",
                },
                json=data,
            )

            commit_response = commit_response.json()
        except Exception as e:
            return {"status": 400, "response": e}

        return {"status": 200, "response": commit_response}

    def create_pull_request(
        self,
        repository_name: str,
        feature_branch_name: str,
        target_branch_name: str = "develop",
        pr_title: Optional[str] = "Pull Request",
        pr_body: Optional[str] = "",
    ):
        """This method will create a pull request based on a repository, target branch and a feature branch passed as method parameters.

        Args:
            repository_name (str): The repository where the branch pull request will be created
            feature_branch_name (str): The branch to be merged on target branch.
            target_branch_name (str, optional): The branch where the feature branch will be merged. Defaults to "develop".
            pr_title (Optional[str], optional): Title of the pull request. Defaults to "Pull Request".
            pr_body (Optional[str], optional): Body message of pull request. Defaults to "".

        Returns:
            Json: Json with the status of the request, if success we will receive 200, if failure we will receive 400 and the stacktrace
        """

        if repository_name not in self.repositories:
            raise Exception(
                "This repository is not available.\n"
                f"Please choose one of the valid options on { self.repositories }:"
            )

        # Create the pull request payload

        feature_branch_name = feature_branch_name.replace(" ", "_")
        target_branch_name = target_branch_name.replace(" ", "_")

        try:
            payload = {
                "title": pr_title,
                "body": pr_body,
                "head": feature_branch_name,
                "base": target_branch_name,
            }

            # Convert the payload to JSON
            payload_json = json.dumps(payload)

            # Make a POST request to create the pull request
            url = (
                f"https://api.github.com/repos/{ self.owner }/{ repository_name }/pulls"
            )

            pull_request_response = requests.post(
                url,
                data=payload_json,
                headers={
                    "Authorization": f"token { self.token }",
                    "Accept": "application/vnd.github+json",
                },
            )

        except Exception as e:
            return {"status": 400, "response": pull_request_response}

        return {"status": 200, "response": pull_request_response}

    def get_git_content(self, repository_name: str, path: str):
        """Retrieve content of a file from a GitHub repository.

        Args:
            repository_name (str): The name of the GitHub repository.
            path (str): The path of the file within the repository.

        Returns:
            dict or None: If successful, the content of the file in JSON format is returned as a dictionary.
                        If the request fails or the content is not in JSON format, returns None.
        """
        url = f"https://raw.githubusercontent.com/{ self.owner }/{ repository_name }/develop/{path}"

        response = requests.get(
            url,
            headers={"Authorization": "Token " + self.token},
        )

        if response.status_code == 200:
            data = json.loads(response.content)
            return data
        else:
            return None
