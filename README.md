# Render scripts

This directory contains Python scripts that are used by engine apps in the accsyn Render farm feature/add-on.

## Development

We recommend you fork of this repository and add customisations to your fork. This way you can easily pull in updates from the main repository.

Source location:

```
    https://github.com/accsyn/compute-scripts.git
```

    
## Setup

1. Create your own repository.

2. Then add this repository as a remote:

    ```
    git remote add upstream https://github.com/accsyn/compute-scripts
    ```
   
3. Pull in the latest changes from the main repository:

    ```
    git pull upstream main
   ```
   

Repeat this step whenever you want to pull in the latest changes from the main repository.


## Deployment

1. Logon to your accsyn workspace as an administrator.

2. Go to the "Admin" pages and then "Engines" menu.

3. First create the Common app that all render apps will use. This is a Python app that contains the common code that all render apps will use. Choose "Create app engine" and then paste the common.py template into the editor, give it the name(code) "common" and finally publish the app.

4. Then create the render app. Choose "Create app engine" and then paste the <app code>.py template into the editor, give it the name(code) and optional color/description (included in script settings), and finally publish the app.

5. To make an engine available on one or more servers: logon to the accsyn app, go to "Manage farm", expand the server to view lanes or switch to lane view, then right click the engine and select "Apps">"<app code>">"Installed".


## Management

Manage servers and engines through the accsyn Desktop app, logged in as an administrator, at the "Manage farm" tab.


## Further resources

Find more information about accsyn at [accsyn.com](https://accsyn.com).

Get support, tutorials, manuals and other useful information at [support.accsyn.com](https://support.accsyn.com).

