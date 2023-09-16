# Data Visualisation Course Template

## About this repository
- This is a template to get you started with your data story. 
- The data in this starter template is taken from Statbel, and involves Belgian accidents in 2021. 
- If you don't have a dataset yet, and you need inspiration for potential datasets, see [this link](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4567555). You can also use the Statbel datasets if they suit your curiosity. 

## Setting up the Databricks workspace
### 1. Creating your private Github Repository
- Fork or clone this [repository](https://github.com/quintenrosseel/data_vis_intro) into your own Github account.
  - Make sure to make your repository private. We don't want to make your datasets public. 
- Go to `settings` > `developer settings` > `tokens (classic)` and create a classic token to link your repository in `step 2`. 
  - Select `repo` to enable git syncing with the repository from within Databricks. 
  - Copy the user name & token and store it somewhere safe. Don't share this token! 

### 2. Adding the repository to the Databricks workspace*
**Login to the Databricks Workspace** 
- Go to [this Databricks workspace](https://adb-328762784338542.2.azuredatabricks.net/)
  - Login with your email (let me know if this doesn't work)
**Add the repository to the Databricks workspace**
- Link your Github with your Github username and the token you generated above. 
  - Click on `<your email>` (top right) > `User Settings` >  `Linked Accounts` 
  - Link your github account with the token you created in `step 1`. (let me know if you encounter issues)
- Add the repository you created under `Workspace` > `Repos` > `<your email>` > `Add` (Top Right) > `Repo`
  - Copy the Git URL (to your private repo) in the URL box. Choose a custom repo name if you like. 
  - The Miro board [Miro board](https://miro.com/app/board/uXjVMoBkTRQ=/?share_link_id=950119977618) also describes this process.

### 3. Add your own dataset
- Go to `/start_here` to see how you can add your dataset to this enviroment. 
- Import your dataset using the Databricks UI 
- Start exploring your data, and maybe get some inspiration from
