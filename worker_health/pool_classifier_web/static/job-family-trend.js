const suiteColors = new Map([
    ["Page load", "#ffac59"], ["Benchmarks", "#65c5ff"], ["Video playback", "#d7a5ff"],
    ["Startup", "#83dd83"], ["Networking", "#ff829f"], ["Resource and power", "#f3dd6c"],
    ["Mochitest media", "#5fdfd0"], ["Mochitest", "#a0afee"], ["JS jitest", "#ffa998"],
    ["WebGL", "#cbdb77"], ["WebRender", "#86d3a9"], ["Crashtest", "#ef8ac0"],
    ["Reftest", "#bda6ee"], ["Xpcshell", "#f3be7a"], ["Web platform tests", "#80d0e0"],
    ["GTest", "#f18eb5"], ["Marionette", "#9bd787"], ["Test Verify", "#e1a2f2"], ["Source tests", "#f0bd72"],
    ["Talos", "#d5e090"], ["Other performance tests", "#81adff"], ["Builds", "#c7cb80"], ["Other tests", "#e5a3ca"],
]);

const number = new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 });
const timeLabel = new Intl.DateTimeFormat(undefined, {
    hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZone: "UTC",
});
const dateLabel = new Intl.DateTimeFormat(undefined, {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZone: "UTC",
});
const dayLabel = new Intl.DateTimeFormat(undefined, { month: "short", day: "numeric", timeZone: "UTC" });
const colorOf = suite => suiteColors.get(suite) ?? "#b8bec9";

export function createJobFamilyTrend(root) {
    root.className = "job-family-trend";
    root.innerHTML = `
        <div class="job-family-trend-heading">
            <h3>Average workers occupied</h3>
            <span class="gen" data-bucket-size></span>
        </div>
        <canvas tabindex="0" aria-label="Worker activity by suite. Use the arrow keys to inspect time buckets."
            aria-describedby="job-family-trend-coverage"></canvas>
        <ul class="job-family-trend-legend" aria-label="Suites shown in the chart"></ul>
        <p class="gen" id="job-family-trend-coverage">Runs appear after finishing. Dashed lines and open dots indicate incomplete collection coverage.</p>
        <p class="gen job-family-trend-inspection" aria-live="polite"></p>`;
    const canvas = root.querySelector("canvas");
    const context = canvas.getContext("2d");
    if (!context) return null;
    const legend = root.querySelector("ul");
    const sizeLabel = root.querySelector("[data-bucket-size]");
    const inspection = root.querySelector(".job-family-trend-inspection");
    let trend;
    let suites = [];
    let visibleSuites = [];
    let selected;
    let focusedBin = null;
    let axis = null;

    function inspect(index, announce = false) {
        focusedBin = index;
        inspection.setAttribute("aria-live", announce ? "polite" : "off");
        if (index === null) {
            inspection.textContent = "";
        } else {
            const bucket = trend.buckets[index];
            const covered = bucket.covered;
            const start = new Date(bucket.start_at);
            const end = new Date(start.getTime() + trend.bucket_seconds * 1000);
            const details = visibleSuites.flatMap(suite => {
                const seconds = bucket.worker_seconds[suite] ?? 0;
                return covered || seconds > 0 ? [`${suite} ${number.format(seconds / trend.bucket_seconds)}`] : [];
            });
            inspection.textContent = [
                `${dateLabel.format(start)}–${timeLabel.format(end)} UTC`,
                ...details,
                ...(!covered ? ["Incomplete collection coverage"] : []),
            ].join(" · ");
        }
        draw();
    }

    function draw() {
        if (!trend || root.hidden) return;
        const width = canvas.clientWidth;
        const height = canvas.clientHeight;
        if (!width || !height) return;
        const ratio = window.devicePixelRatio || 1;
        canvas.width = Math.round(width * ratio);
        canvas.height = Math.round(height * ratio);
        context.scale(ratio, ratio);
        const { buckets, bucket_seconds: bucketSeconds } = trend;
        const left = 38;
        const right = width - 18;
        const top = 12;
        const bottom = height - 27;
        const bucketWidth = (right - left) / buckets.length;
        const x = index => left + (index + .5) * bucketWidth;
        axis = { left, bucketWidth };
        const peak = Math.max(1, trend.scale_peak_workers);
        const magnitude = 10 ** Math.floor(Math.log10(peak / 4));
        const step = magnitude * [1, 2, 2.5, 5, 10].find(factor => factor * magnitude >= peak / 4);
        const ceiling = step * Math.ceil(peak / step);
        const y = workers => bottom - workers * (bottom - top) / ceiling;
        const narrow = width < 520;

        context.font = "12px monospace";
        context.textBaseline = "middle";
        context.textAlign = "right";
        context.fillStyle = "#bbb";
        context.strokeStyle = "#373737";
        context.lineWidth = 1;
        for (let tick = 0; tick <= Math.round(ceiling / step); tick++) {
            const value = tick * step;
            const position = y(value);
            context.fillText(number.format(value), left - 6, position);
            context.beginPath();
            context.moveTo(left, position);
            context.lineTo(right, position);
            context.stroke();
        }

        context.textBaseline = "top";
        const tickIndexes = new Set([0, Math.floor(buckets.length / 2), buckets.length - 1]);
        if (!narrow) {
            tickIndexes.add(Math.floor(buckets.length / 4));
            tickIndexes.add(Math.floor(3 * buckets.length / 4));
        }
        for (const index of tickIndexes) {
            const edge = index === 0 || index === buckets.length - 1;
            const formatter = bucketSeconds > 3600
                ? (narrow ? dayLabel : dateLabel)
                : (edge && !narrow ? dateLabel : timeLabel);
            context.textAlign = index === 0 ? "left" : index === buckets.length - 1 ? "right" : "center";
            const position = index === 0 ? left : index === buckets.length - 1 ? right : x(index);
            context.fillText(formatter.format(new Date(buckets[index].start_at)), position, bottom + 8);
        }

        if (focusedBin !== null) {
            context.strokeStyle = "#777";
            context.beginPath();
            context.moveTo(x(focusedBin), top);
            context.lineTo(x(focusedBin), bottom);
            context.stroke();
        }

        const drawOrder = visibleSuites.filter(suite => suite !== selected).concat(selected);
        for (const suite of drawOrder) {
            const color = colorOf(suite);
            context.strokeStyle = color;
            context.fillStyle = color;
            context.lineWidth = suite === selected ? 2.75 : 1.6;
            context.globalAlpha = suite === selected ? 1 : .72;
            const points = buckets.map(bucket => {
                const seconds = bucket.worker_seconds[suite] ?? 0;
                return bucket.covered || seconds > 0 ? y(seconds / bucketSeconds) : null;
            });
            for (const incomplete of [false, true]) {
                context.setLineDash(incomplete ? [5, 4] : []);
                context.beginPath();
                for (let index = 1; index < points.length; index++) {
                    if (points[index - 1] === null || points[index] === null) continue;
                    const gap = !buckets[index - 1].covered || !buckets[index].covered;
                    if (gap !== incomplete) continue;
                    context.moveTo(x(index - 1), points[index - 1]);
                    context.lineTo(x(index), points[index]);
                }
                context.stroke();
            }
            context.setLineDash([]);
            for (const [index, bucket] of buckets.entries()) {
                const isolated = points[index - 1] == null && points[index + 1] == null;
                const open = !bucket.covered;
                if (points[index] === null || (!open && !isolated && index !== focusedBin)) continue;
                context.beginPath();
                context.arc(x(index), points[index], open ? 3.25 : 2.5, 0, Math.PI * 2);
                if (open) context.stroke();
                else context.fill();
            }
        }
        context.globalAlpha = 1;
    }

    function select(suite) {
        selected = suite;
        visibleSuites = suites.slice(0, 5);
        if (!visibleSuites.includes(suite)) visibleSuites.push(suite);
        legend.replaceChildren();
        for (const name of visibleSuites) {
            const item = document.createElement("li");
            const swatch = document.createElement("span");
            swatch.setAttribute("aria-hidden", "true");
            swatch.style.backgroundColor = colorOf(name);
            item.append(swatch, name);
            item.classList.toggle("selected", name === suite);
            legend.append(item);
        }
        inspect(focusedBin);
    }

    function pointAt(event) {
        if (!axis) return;
        const offset = event.clientX - canvas.getBoundingClientRect().left - axis.left;
        const index = Math.max(0, Math.min(trend.buckets.length - 1, Math.floor(offset / axis.bucketWidth)));
        if (index !== focusedBin) inspect(index);
    }

    canvas.addEventListener("pointermove", pointAt);
    canvas.addEventListener("pointerdown", pointAt);
    canvas.addEventListener("pointerleave", () => inspect(null));
    canvas.addEventListener("blur", () => inspect(null));
    canvas.addEventListener("keydown", event => {
        if (!trend || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
        event.preventDefault();
        const last = trend.buckets.length - 1;
        const next = {
            Home: 0, End: last,
            ArrowLeft: Math.max(0, (focusedBin ?? last + 1) - 1),
            ArrowRight: Math.min(last, (focusedBin ?? -1) + 1),
        };
        inspect(next[event.key], true);
    });
    new ResizeObserver(draw).observe(canvas);

    return {
        select,
        render(data, names, suite) {
            trend = data;
            suites = names;
            focusedBin = null;
            const duration = data.bucket_seconds < 3600
                ? `${data.bucket_seconds / 60} min` : `${data.bucket_seconds / 3600}h`;
            sizeLabel.textContent = `${duration} per point · UTC${names.length > 5 ? " · Top 5 and selected suite" : ""}`;
            select(suite);
        },
    };
}
