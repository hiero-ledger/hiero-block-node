# Hiero Block Node Cloud Deployment Guide

# Overview

This guide explains how to deploy the Hiero Block Node on a Google Cloud Platform (GCP) virtual machine (VM) using the Solo Weaver tool.
The Block Node enables participation in the Hiero network by processing and validating transactions.
This deployment uses the Weaver binary to provision the VM, set up Kubernetes and install the Block Node Helm chart on the VM.

This guide walks you through creating the VM, uploading the Weaver binary to it, and running the Block Node using the local profile.

While this guide focuses on GCP, node operators can deploy the Block Node on any cloud provider of their choice by following equivalent provisioning and deployment steps.

# Prerequisites

Before you begin, ensure you have:

- Access to a cloud provider account (such as Google Cloud, AWS, or Azure) with permissions to create and manage VM instances.
- The Weaver binary downloaded locally from the [**official Weaver releases page**](https://github.com/hashgraph/solo-weaver/releases) or provided as part of your node onboarding for your VM’s architecture:
  - **`weaver-linux-amd64`** for x86_64 (most standard cloud VMs).
  - **`weaver-linux-arm64`** for ARM-based VMs.
- The **`gcloud`** CLI installed and authenticated (if using Google Cloud).

# Step-by-Step Guide

## Step 1: Create a Google Cloud VM

1. In the [**Google Cloud Console**](https://console.cloud.google.com/), click **Select a project** at the top of the page.
2. Choose an existing project. If you don’t have a project, click **New project** in the popup window and follow the prompts to create one.
3. Under **Compute Engine,** Select **Create a VM**.
4. Select a machine type appropriate for your Block Node profile:
   - **For a test or local profile**: Choose at least an **E2 standard** machine (for example, **`e2-standard-2`**) so that CPU and memory are sufficient.
   - **For production (f**e.g.**, `mainnet` or `previewnet`)**: Select at least ~16 CPUs (e.g., 8 physical cores / 16 vCPUs).
5. Set the region and zone (defaults are fine unless you have a preference).
6. Select the default boot disk and operating system unless your team has specific requirements.
7. Leave other instance settings at defaults for a standard deployment.
8. Click **Create** to launch the VM.
9. Wait until the instance status is **Running** before proceeding.

## Step 2: Upload Weaver to the VM

1. Download the appropriate Weaver binary to your local machine (see [Prerequisites](https://www.notion.so/Hiero-Block-Node-Cloud-Deployment-Guide-2b77c9ab259180fa9cc0e111cf9f77d0?pvs=21) for the correct file).
2. In the Google Cloud Console, open your VM’s details page.
3. Select the **down arrow** next to **SSH**, then choose **View gcloud command**.
4. Copy the suggested **`gcloud compute ssh`** command displayed.
5. On your local machine, paste that command into a terminal. It will look similar to:

   ```bash
   gcloud compute ssh --zone <ZONE> <INSTANCE_NAME> --project <PROJECT_ID>
   ```
6. On your local machine, modify the copied command:
   - Change **`ssh`** to **`scp`**.
   - Set the **source path** to your Weaver binary (e.g., **`~/Downloads/weaver-linux-amd64`**).
   - Set the **destination path** to your VM (e.g., **`/home/<USER>/weaver`**).

   Example command (update with your details):

   ```bash
   gcloud compute scp ~/Downloads/weaver-linux-amd64 <INSTANCE_NAME>:/home/<USER>/weaver --zone <ZONE> --project <PROJECT_ID>
   ```
7. Run the command to upload the file.
8. When complete, the Weaver binary will be available on your VM at the specified path. (e.g., at **`/home/<USER>/weaver`).**

## Step 3: Connect to VM and Prepare Weaver

1. SSH into your VM:
   - Use the **SSH** button in the Google Cloud Console (browser window), or
   - Open a separate terminal on your local machine and run:

     ```bash
     gcloud compute ssh <INSTANCE_NAME> --zone=<ZONE> --project=<PROJECT_ID>
     ```
   - After connecting, run **`ls`** to confirm the Weaver binary is present in your home directory.
2. Make the binary executable, using the correct filename:

   ```bash
   chmod +x weaver-linux-amd64
   ```

   *(Use **`weaver-linux-arm64`** for ARM)*

3. Test the Weaver binary by running it without arguments:

   ```bash
   ./weaver-linux-amd64
   ```

   *(or **`./weaver-linux-arm64`** for ARM)*

   If the binary runs and prints usage or help text, Weaver is correctly installed on the VM.

## Step 4: Create the **`weaver`** User

1. Run the Block Node setup command with **`sudo`** and the desired profile (choose one of: **`local`**, **`testnet`**, **`previewnet`**, **`mainnet`**):

   ```bash
   sudo ./weaver-linux-amd64 blocknode setup -p <profile>
   ```

   *(Replace **`<profile>`** with the desired value, e.g., **`local`** for test deployments. For ARM, use **`weaver-linux-arm64`** instead.)*

2. The command will fail and print instructions to create a **`weaver`** system user and group.

   - Copy the **`groupadd`** and **`useradd`** commands shown in the output.
   - Run them exactly as printed, including specific user/group ID numbers.
3. After creating the **`weaver`** user and group, re-run the setup command:

   ```bash
   sudo ./weaver-linux-amd64 blocknode setup -p <profile>
   ```

Once complete, Weaver will be able to manage Kubernetes resources on your VM using the dedicated **`weaver`** system user.

## Step 5: Run Block Node Setup with the Local Profile

1. Ensure you are on the VM and the **`weaver`** binary is executable.
2. Run the Block Node setup with **`sudo`** and the local profile:

   ```bash
   sudo ./weaver-linux-amd64 blocknode setup -p <profile>
   ```

   Replace **`<profile>`** with one of: **`local`**, **`testnet`**, **`previewnet`**, or **`mainnet`**.

3. **What the setup does:**

   - Perform system and resource checks.
   - Install and configure Kubernetes components.
   - Install MetalLB for load balancing.
   - Deploy the Block Node Helm chart into the cluster.
4. The process takes several minutes and will show progress logs in your terminal.

## Step 6: Verify the Block Node Deployment

After completing the setup, confirm that your Block Node is deployed and running by checking the Kubernetes cluster:

1. Using **`kubectl`** (recommended)
   1. Weaver configures local Kubernetes access on the VM. From your VM terminal, run:

      ```bash
      kubectl get pods -A
      ```
   2. Look for Block Node pods. Ensure they show **`Running`** status and all containers are ready (e.g., **`1/1`** or **`2/2`**).
2. Using K9s (optional)

- If you prefer a text-based Kubernetes dashboard:
  1. [**Install `k9s`**](https://k9scli.io/) on your VM or a machine with access to the cluster.
  2. To Inspect pods, namespaces, and logs. Run:

     ```bash
     k9s
     ```
  3. Confirm the Block Node `StatefulSet/Pods` are healthy.

If the pods are running and healthy, your Block Node is successfully installed and running on the Google Cloud VM.

## Step 7: Deprovisioning and Shutdown

If you need to permanently remove a Block Node deployment (for decommissioning, upgrades, or migration):

- **Test environments:** Delete the VM from the Compute Engine page in Google Cloud Console to remove all associated resources.
- **Production nodes:** Follow organizational and project-specific procedures for graceful shutdown, backup, and ongoing monitoring to avoid service interruption or data loss.

## Troubleshooting

See below for common errors, causes, and solutions during Block Node setup:

1. Error: “Profile not set”
   - **Cause:** The **`blocknode setup`** command was run without specifying a profile.
   - **Fix:** Re-run with a valid profile, for example:

     ```bash
     sudo ./weaver-linux-amd64 blocknode setup -p local
     ```
2. Error: “Requires super user privilege”
   - **Cause:** The **`blocknode setup`** command was run without **`sudo`**.
   - **Fix:** Add **`sudo`** before your command:

   ```bash
   sudo ./weaver-linux-amd64 blocknode setup -p local
   ```
3. Error: Missing **`weaver`** User and/or Group
   - **Cause:** First **`sudo`** run; the **`weaver`** system user and group are not yet created.
   - **Fix:** Run the **`groupadd`** and **`useradd`** commands shown in the error output, then re-run the setup.
4. Error: “CPU does not meet Block Node requirements” or “Insufficient memory”
   - **Cause:** The VM machine type is too small for the selected profile.
   - **Fix:**
     - Delete your current VM.
     - Create a new VM with a larger machine type (e.g., **`e2-standard-2`** or higher for **`local`**; higher specs for production profiles).
     - Repeat the installation steps.
