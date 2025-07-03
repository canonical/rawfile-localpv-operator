# Copyright 2025 Canonical Ltd.
# See LICENSE file for licensing details.

"""Helm Template Processing Script."""

import logging
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple

from packaging.version import InvalidVersion, Version

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


CONFIG = {
    "repo_url": "https://github.com/canonical/rawfile-localpv.git",
    "tag_prefix": "rawfile-csi-",
    "start_version": "0.8.2",
    "excluded_versions": ["0.9.0"],
    "helm_chart_path": "deploy/charts/rawfile-csi",
    "output_manifest_name": "rawfile-localpv.yaml",
    "output_base_dir": "upstream/manifests",
    "values_file": "values.yaml",
}


class GitRepository:
    """Handles git repository ops."""

    def __init__(self, repo_url: str, local_path: str):
        self.repo_url = repo_url
        self.local_path = Path(local_path)

    def clone(self) -> None:
        """Clone repository."""
        print(f"Cloning repository from {self.repo_url}...")
        subprocess.check_call(
            ["git", "clone", self.repo_url, str(self.local_path)],
            text=True,
        )
        print(f"Clone successful to {self.local_path}")

    def get_tags(self) -> List[str]:
        """Get all tags from the repository.

        Returns:
            List of tag names
        """
        result = subprocess.run(
            ["git", "tag", "-l"],
            cwd=self.local_path,
            capture_output=True,
            text=True,
            check=True,
        )
        tags = result.stdout.strip().split("\n")
        return [tag for tag in tags if tag]

    def checkout_tag(self, tag: str):
        """Checkout a specific tag.

        Args:
            tag: Tag name to checkout
        """
        subprocess.run(
            ["git", "checkout", tag],
            cwd=self.local_path,
            capture_output=True,
            text=True,
            check=True,
        )
        print(f"Checked out tag: {tag}")


class HelmProcessor:
    """Handles Helm template generation operations."""

    @staticmethod
    def generate_template(chart_path: Path, output_file: Path, values_file: Optional[Path] = None):
        """Generate Helm template and save to a file.

        Args:
            chart_path: Path to the Helm chart directory
            output_file: Path where to save the generated template.
            values_file: Path to the custom Helm values used to render the template.
        """
        output_file.parent.mkdir(parents=True, exist_ok=True)

        if not chart_path.exists():
            raise RuntimeError(f"Helm chart path missing: {chart_path}")

        cmd = 'helm template . --name-template "changeme"'
        if values_file and values_file.exists():
            cmd += f" -f {str(values_file)}"

        result = subprocess.run(
            shlex.split(cmd),
            cwd=chart_path,
            capture_output=True,
            text=True,
            check=True,
        )

        output = result.stdout
        # NOTE: 'changeme' is the release name, the charm
        # takes care of adding the required prefixes to
        # differentiate each deployment.
        output = output.replace("changeme-", "")

        with open(output_file, "w") as f:
            f.write(output)

        print(f"Generated template saved to: {output_file}")


class ManifestsProcessor:
    """Main processor class for manifest updates."""

    def __init__(self, config: dict):
        self.config = config
        self.original_dir = Path.cwd()
        self.script_dir = Path(__file__).parent.absolute()

        self.values_file = self.script_dir / config["values_file"]
        if self.values_file.exists():
            print(f"Found values file: {self.values_file}")
        else:
            print(f"No values file found at: {self.values_file}")
            self.values_file = None

    def filter_tags(self, tags: List[str]) -> List[Tuple[str, str]]:
        """Filter tags based on prefix, versions and exclusions.

        Args:
            tags: List of all tags

        Returns:
            List of tuples (tag_name, version) for selected tags
        """
        filtered_tags = []
        tag_prefix = self.config["tag_prefix"]
        start_version = self.config["start_version"]
        excluded_versions = self.config["excluded_versions"]

        try:
            start_version_obj = Version(start_version)
        except InvalidVersion:
            print(f"Invalid start version: {start_version}")
            return []

        for tag in tags:
            if not tag.startswith(tag_prefix):
                continue

            version_str = tag[len(tag_prefix) :]
            try:
                version_obj = Version(version_str)
                if version_obj < start_version_obj:
                    continue
                if version_str in excluded_versions:
                    print(f"Skipping excluded version: {version_str}")
                    continue

                filtered_tags.append((tag, version_str))

            except InvalidVersion:
                print(f"Skipping tag {tag} due to invalid version format: {version_str}")
                continue

        filtered_tags.sort(key=lambda x: Version(x[1]))

        return filtered_tags

    def process_tag(self, repo: GitRepository, tag: str, version: str):
        """Process a single tag: checkout, generate template, save output.

        Args:
            repo: GitRepository instance
            tag: Tag name
            version: Version string
        """
        print(f"\nProcessing tag: {tag} (version: {version})")

        repo.checkout_tag(tag)

        chart_path = repo.local_path / self.config["helm_chart_path"]
        output_dir = self.original_dir / self.config["output_base_dir"] / version
        output_file = output_dir / self.config["output_manifest_name"]

        HelmProcessor.generate_template(chart_path, output_file, self.values_file)

    def run(self):
        """Execute the update process."""
        print("Updating manifests...")
        print(f"Repository URL: {self.config['repo_url']}")
        print(f"Start version: {self.config['start_version']}")
        print(f"Excluded versions: {self.config['excluded_versions']}")

        with tempfile.TemporaryDirectory() as temp_dir:
            repo_path = Path(temp_dir) / "repo"
            repo = GitRepository(self.config["repo_url"], str(repo_path))

            repo.clone()
            tags = repo.get_tags()

            if not tags:
                raise RuntimeError("No tags found in repository")

            print(f"Found {len(tags)} total tags")

            selected_tags = self.filter_tags(tags)

            if not selected_tags:
                print("No tags match the filtering criteria")
                return False

            print(f"Selected {len(selected_tags)} tags for processing:")
            for tag, version in selected_tags:
                print(f"  - {tag} ({version})")

            for tag, version in selected_tags:
                self.process_tag(repo, tag, version)
                print(f"Tag '{tag}' processed.")


def check_dependencies() -> bool:
    """Check if required external tools are available.

    Returns:
        True if all dependencies are available, False otherwise
    """
    dependencies = ["git", "helm"]

    for dep in dependencies:
        try:
            subprocess.run([dep, "version"], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print(f"Error: {dep} is not installed or not in PATH")
            return False

    return True


def main():
    """Start the script."""
    if not check_dependencies():
        print("Please install missing system dependencies and try again.")
        sys.exit(1)

    try:
        processor = ManifestsProcessor(CONFIG)
        processor.run()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
