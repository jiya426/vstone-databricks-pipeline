# Git workflow

Branches: `main` (production), `dev` (integration), and short-lived feature branches.

## 1. Start a change
1. Update `dev`: `git checkout dev && git pull`
2. Create a branch named for the change: `git checkout -b feature/<short-name>`
   Examples: `feature/region-sk`, `feature/readme`, `feature/docs-region-sk`.

## 2. Commit
- Keep each commit to one change.
- Write the message as an imperative sentence: "Add region_sk version key", not "added".
- Commit and push: `git add -A && git commit -m "<message>" && git push -u origin feature/<short-name>`

## 3. Open a pull request into `dev`
- Base: `dev`. Compare: your feature branch.
- Give it a title that says what changes, and a short description listing the files and the tests run.
- The CI checks run on the PR: unit tests and bundle validation must pass before merging.

## 4. Verify in Databricks
- For changes to pipelines, jobs or notebooks: run the master pipeline from GitHub Actions
  (Actions → `databricks-ci-cd` → Run workflow → pick your feature branch).
- Check the Gold checks (`ops.sanity_check_results` has no FAIL rows for the new run) and the dashboards.

## 5. Merge into `dev`
- Merge only after the checks pass and step 4 is green.
- Merging into `dev` runs the dev deploy and the jobs (`Deploy to dev and run the jobs`).
- Delete the feature branch after merging.

## 6. Release to `main`
- Open a PR from `dev` into `main`, after every change in `dev` is merged.
- Merging into `main` runs the production deploy job (`Deploy to prod`).
- Only release when the evaluation requires it.

## Rules
- Never commit directly to `dev` or `main`.
- Never use "Run now" in Databricks. Jobs run only from GitHub Actions (assumption A19).
- Never merge a PR with a failing check.
