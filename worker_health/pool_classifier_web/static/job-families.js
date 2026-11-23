(() => {
    const section = document.getElementById("job-families");
    if (!section) return;

    // Stored pool pages can still contain the previous table markup.
    const mount = section.querySelector("[data-suite-explorer], table");
    const explorer = document.createElement("div");
    explorer.className = "job-suite-explorer";
    explorer.hidden = true;
    explorer.innerHTML = `
        <div>
            <div class="job-suite-list-heading"><span>Suite</span><span>Share of time</span></div>
            <ul class="job-suite-list" aria-label="Suites by share of worker time"></ul>
        </div>
        <section id="job-suite-detail" class="job-suite-detail" aria-labelledby="job-suite-name">
            <div aria-live="polite">
                <h3 id="job-suite-name"></h3>
                <p class="gen"></p>
            </div>
            <table class="not-sortable" aria-label="Jobs in selected suite">
                <thead><tr><th scope="col">Job name</th><th scope="col">Runs</th><th scope="col">Worker-hours</th></tr></thead>
                <tbody></tbody>
            </table>
        </section>`;
    mount.replaceWith(explorer);
    const stylesheet = document.createElement("link");
    stylesheet.rel = "stylesheet";
    stylesheet.href = "/static/job-families.css";
    document.head.append(stylesheet);

    const status = section.querySelector('[role="status"]');
    const periodButtons = [...section.querySelectorAll("button[data-days]")];
    const suiteList = explorer.querySelector(".job-suite-list");
    const detailHeading = explorer.querySelector("#job-suite-name");
    const detailSummary = explorer.querySelector(".job-suite-detail p");
    const jobRows = explorer.querySelector("tbody");
    const number = new Intl.NumberFormat();
    const percent = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 1 });
    const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
    const hours = seconds => decimal.format(seconds / 3600);
    const runsLabel = runs => `${number.format(runs)} ${runs === 1 ? "run" : "runs"}`;
    let selectedName;
    let activeButton;
    let request;

    function select(family, button) {
        activeButton?.setAttribute("aria-pressed", "false");
        button.setAttribute("aria-pressed", "true");
        activeButton = button;
        selectedName = family.name;
        detailHeading.textContent = family.name;
        detailSummary.textContent = `${runsLabel(family.runs)} · ${hours(family.seconds)} worker-hours`;
        jobRows.replaceChildren();
        for (const job of family.jobs) {
            const row = jobRows.insertRow();
            row.insertCell().textContent = job.task_name ?? "Unknown job name";
            row.insertCell().textContent = number.format(job.runs);
            row.insertCell().textContent = hours(job.worker_seconds);
        }
    }

    function render(jobs) {
        const families = new Map();
        let runs = 0;
        let seconds = 0;
        for (const job of jobs) {
            if (!families.has(job.job_family)) {
                families.set(job.job_family, { name: job.job_family, runs: 0, seconds: 0, jobs: [] });
            }
            const family = families.get(job.job_family);
            family.runs += job.runs;
            family.seconds += job.worker_seconds;
            family.jobs.push(job);
            runs += job.runs;
            seconds += job.worker_seconds;
        }

        if (!runs) {
            status.textContent = "No observed finished runs with execution time in this period.";
            return;
        }

        const ordered = [...families.values()].sort((a, b) => b.seconds - a.seconds || a.name.localeCompare(b.name));
        const initial = families.get(selectedName) ?? ordered[0];
        suiteList.replaceChildren();
        activeButton = undefined;
        for (const family of ordered) {
            const item = document.createElement("li");
            const button = document.createElement("button");
            button.type = "button";
            button.setAttribute("aria-pressed", "false");
            button.setAttribute("aria-controls", "job-suite-detail");
            const name = document.createElement("span");
            const share = document.createElement("span");
            name.textContent = family.name;
            share.textContent = seconds > 0 ? percent.format(family.seconds / seconds) : "—";
            button.append(name, share);
            button.addEventListener("click", () => select(family, button));
            item.append(button);
            suiteList.append(item);
            if (initial === family) select(family, button);
        }
        status.textContent = `Observed finished runs · ${runsLabel(runs)} · ${hours(seconds)} worker-hours in this period`;
        explorer.hidden = false;
    }

    async function load(selected) {
        request?.abort();
        request = new AbortController();
        const { signal } = request;
        for (const button of periodButtons) {
            button.classList.toggle("active", button === selected);
            button.setAttribute("aria-pressed", String(button === selected));
        }
        status.textContent = "Loading jobs by suite…";
        explorer.hidden = true;
        try {
            const response = await fetch(`${section.dataset.endpoint}?days=${selected.dataset.days}`, { signal });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            if (!signal.aborted) render(data.jobs);
        } catch (error) {
            if (!signal.aborted) status.textContent = `Jobs by suite unavailable (${error.message}).`;
        }
    }

    for (const button of periodButtons) button.addEventListener("click", () => load(button));
    load(periodButtons[0]);
})();
