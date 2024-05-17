# elecciones-salvador

Scraping process to download each summary vote totals (JPG or PDF files) published by the Salvador government. There are 16 million votes approximately and the summary vote totals will be used by the independent project of the Guatemalan non-profit organization [FundacionHCG.org](https://fundacionhcg.org/) and [FiscalDigital.net](https://fiscaldigital.net/)

## Requirements

* Python 3.8+
* Poetry 1.7+
    - [Install Poetry](https://python-poetry.org/docs/#installation)
* Chrome Browser for Testing
    - [Stable](https://googlechromelabs.github.io/chrome-for-testing/#stable)
* Chrome Driver for Testing
    - [Stable](https://googlechromelabs.github.io/chrome-for-testing/#stable)


## How to use

Please read and execute each step below:

### Step 1

Add Poetry to your PATH:

```bash
$promt> export PATH="$HOME/.local/bin:$PATH"
```

Also you can add Poetry to your .bashrc file:

```bash
$promt> nano ~/.bashrc
```

Install poetry by script:

```bash
$promt> ./install-poetry.sh
```

### Step 2

Command to tell Poetry which Python version to use for the current project:

```bash
$promt> poetry env use 3.12
```

### Step 3

Activating the virtual environment:

```bash
$promt> poetry shell
```

### Step 4

Installing dependencies:

```bash
$(elecciones-salvador-py3.12)> poetry install --no-root
```

### Step 5

Download both Chrome App and Driver for Testing and copy each one in the specific folder:

- Chrome App: **.src/scraping/browser/app**
- Chrome Driver: **.src/scraping/browser/driver**

### Optional

Displaying the environment information:

```bash
$promt> poetry env info
```

Adds required packages to your pyproject.toml and installs them:

```bash
$promt> poetry add boto3
```

Deactivate the virtual environment and exit:

```bash
$(elecciones-salvador-py3.12)> exit
# To deactivate the virtual environment without leaving the shell use deactivate
$(elecciones-salvador-py3.12)> deactivate
```

---

## Web Scraping - Elecciones Salvador

A base code was created inside the folder **src** please check out the [README.md](src/README.md) file.

---

***That's all for now ...***

---

#### License

[MIT License](./LICENSE)

[CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/?ref=chooser-v1)

#### About me

[https://about.me/sebaxtian](https://about.me/sebaxtian)
