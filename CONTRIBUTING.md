# Contributing to nodejs-polars

First of all, thank you for considering contributing to nodejs-polars!

The following is a set of guidelines for contributing to nodejs-polars. These are just guidelines, not rules, use your best judgment and feel free to propose changes to this document in a pull request.

## How Can I Contribute?

### Reporting Bugs

- **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/pola-rs/nodejs-polars/issues).

- If you're unable to find an open issue addressing the problem, open a new one. Be sure to include a **title and clear description**, as much relevant information as possible, and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

- If possible, use the relevant bug report templates to create the issue.

### Suggesting Enhancements

- Open a new GitHub issue in the [Issues](https://github.com/pola-rs/nodejs-polars/issues) with a **clear title** and **description**.

- If possible, use the relevant enhancement request templates to create the issue.


### Pull Requests

- Fill in the required template
- Use a descriptive title. *(This will end up in the changelog)*
- In the pull request description, link to the issue you were working on.
- Add any relevant information to the description that you think may help the maintainers review your code.
- Make sure your branch is [rebased](https://docs.github.com/en/get-started/using-git/about-git-rebase) against the latest version of the `main` branch.
- Make sure all GitHub Actions checks pass.



## Development

- Fork the repository, then clone it from your fork
```
git clone https://github.com/<your-github-username>/nodejs-polars.git
```

- Install dependencies
```
yarn install
```

- Build the binary
```
yarn build:debug
```

- Run the tests
```
yarn jest
```

- Make your changes
- Test your changes
- Update the documentation if necessary
- Create a new pull request
