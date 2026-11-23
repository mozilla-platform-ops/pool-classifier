(() => {
    const section = document.getElementById("job-families");
    if (!section) return;

    const status = section.querySelector('[role="status"]');
    const buttons = [...section.querySelectorAll("button[data-days]")];
    const table = section.querySelector("table");
    const body = table.tBodies[0];
    const number = new Intl.NumberFormat();
    const percent = new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 1 });
    const decimal = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
    const hours = seconds => decimal.format(seconds / 3600);
    let request;

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

        const ordered = [...families.values()].sort((a, b) => b.runs - a.runs || a.name.localeCompare(b.name));
        for (const family of ordered) {
            const row = body.insertRow();
            const details = document.createElement("details");
            const summary = document.createElement("summary");
            summary.textContent = family.name;
            const names = document.createElement("ul");
            for (const job of family.jobs) {
                const item = document.createElement("li");
                item.textContent = `${job.task_name ?? "Unknown job name"} · ${number.format(job.runs)} runs · ${hours(job.worker_seconds)} h`;
                names.append(item);
            }
            details.append(summary, names);
            row.insertCell().append(details);
            row.insertCell().textContent = number.format(family.runs);
            row.insertCell().textContent = percent.format(family.runs / runs);
            row.insertCell().textContent = hours(family.seconds);
        }
        status.textContent = runs
            ? `Observed finished runs · ${number.format(runs)} runs · ${hours(seconds)} worker-hours in this period`
            : "No observed finished runs with execution time in this period.";
        table.hidden = !runs;
    }

    async function load(selected) {
        request?.abort();
        request = new AbortController();
        const { signal } = request;
        for (const button of buttons) {
            button.classList.toggle("active", button === selected);
            button.setAttribute("aria-pressed", String(button === selected));
        }
        status.textContent = "Loading jobs by suite…";
        table.hidden = true;
        body.replaceChildren();
        try {
            const response = await fetch(`${section.dataset.endpoint}?days=${selected.dataset.days}`, { signal });
            if (!response.ok) throw new Error(`HTTP ${response.status}`);
            const data = await response.json();
            if (!signal.aborted) render(data.jobs);
        } catch (error) {
            if (!signal.aborted) status.textContent = `Jobs by suite unavailable (${error.message}).`;
        }
    }

    for (const button of buttons) button.addEventListener("click", () => load(button));
    load(buttons[0]);
})();
