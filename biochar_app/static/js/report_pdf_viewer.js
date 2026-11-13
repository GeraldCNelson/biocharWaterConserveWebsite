import * as pdfjsLib from "../vendor/pdfjs/pdf.min.mjs";

pdfjsLib.GlobalWorkerOptions.workerSrc = new URL(
  "../vendor/pdfjs/pdf.worker.min.mjs",
  import.meta.url,
).href;

const reader = document.querySelector(".draft-report-reader[data-pdf-url]");

if (reader) {
  const viewportElement = reader.querySelector("[data-pdf-viewport]");
  const pagesElement = reader.querySelector("[data-pdf-pages]");
  const statusElement = reader.querySelector("[data-pdf-status]");
  const pageInput = reader.querySelector("[data-pdf-page]");
  const pageCountElement = reader.querySelector("[data-pdf-page-count]");
  const previousButton = reader.querySelector("[data-pdf-previous]");
  const nextButton = reader.querySelector("[data-pdf-next]");
  const zoomOutButton = reader.querySelector("[data-pdf-zoom-out]");
  const zoomInButton = reader.querySelector("[data-pdf-zoom-in]");
  const fitWidthButton = reader.querySelector("[data-pdf-fit-width]");

  let documentHandle = null;
  let pageNumber = 1;
  let scaleMultiplier = 1;
  let fitWidth = true;
  let resizeTimer = null;
  let scrollFrame = null;
  let lastViewportWidth = 0;
  let renderObserver = null;
  const visiblePages = new Set();
  const pageStates = new Map();

  function updateControls() {
    const ready = Boolean(documentHandle);
    const pageCount = documentHandle?.numPages ?? 1;
    previousButton.disabled = !ready || pageNumber <= 1;
    nextButton.disabled = !ready || pageNumber >= pageCount;
    zoomOutButton.disabled = !ready;
    zoomInButton.disabled = !ready;
    fitWidthButton.disabled = !ready;
    pageInput.disabled = !ready;
    pageInput.value = String(pageNumber);
    pageInput.max = String(pageCount);
    fitWidthButton.classList.toggle("active", ready && fitWidth);
    fitWidthButton.setAttribute("aria-pressed", String(ready && fitWidth));
  }

  function scaleForWidth(baseWidth) {
    const availableWidth = Math.max(viewportElement.clientWidth - 32, 280);
    const fitScale = availableWidth / baseWidth;
    return Math.max(0.5, Math.min(3, fitScale * scaleMultiplier));
  }

  function sizePageShell(state) {
    const scale = scaleForWidth(state.baseWidth);
    state.shell.style.width = `${state.baseWidth * scale}px`;
    state.shell.style.height = `${state.baseHeight * scale}px`;
    return scale;
  }

  function createPageState(number, baseViewport) {
    const shell = document.createElement("section");
    shell.className = "draft-report-canvas-page";
    shell.dataset.pageNumber = String(number);
    shell.setAttribute("aria-label", `Report page ${number}`);

    const canvas = document.createElement("canvas");
    canvas.hidden = true;
    canvas.setAttribute("aria-label", `Rendered report page ${number}`);

    const annotations = document.createElement("div");
    annotations.className = "draft-report-annotation-layer";
    annotations.dataset.pdfAnnotations = "";

    shell.append(canvas, annotations);
    pagesElement.append(shell);

    const state = {
      number,
      shell,
      canvas,
      context: canvas.getContext("2d", { alpha: false }),
      annotations,
      baseWidth: baseViewport.width,
      baseHeight: baseViewport.height,
      renderedScale: null,
      renderTask: null,
    };
    pageStates.set(number, state);
    sizePageShell(state);
    return state;
  }

  function releasePage(number) {
    const state = pageStates.get(number);
    if (!state || state.renderedScale === null) return;
    state.renderTask?.cancel();
    state.renderTask = null;
    state.renderedScale = null;
    state.canvas.width = 1;
    state.canvas.height = 1;
    state.canvas.hidden = true;
    state.annotations.replaceChildren();
  }

  async function followInternalDestination(destination) {
    const explicitDestination = typeof destination === "string"
      ? await documentHandle.getDestination(destination)
      : destination;
    if (!explicitDestination?.[0]) return;
    const destinationPage = (
      await documentHandle.getPageIndex(explicitDestination[0])
    ) + 1;
    await goToPage(destinationPage);
  }

  async function renderAnnotations(page, viewport, annotationsElement) {
    annotationsElement.replaceChildren();
    annotationsElement.style.width = `${viewport.width}px`;
    annotationsElement.style.height = `${viewport.height}px`;
    const annotations = await page.getAnnotations({ intent: "display" });

    for (const annotation of annotations) {
      if (
        annotation.subtype !== "Link" ||
        (!annotation.url && !annotation.dest) ||
        !Array.isArray(annotation.rect) ||
        annotation.rect.length < 4
      ) continue;

      const firstCorner = [annotation.rect[0], annotation.rect[1]];
      const secondCorner = [annotation.rect[2], annotation.rect[3]];
      pdfjsLib.Util.applyTransform(firstCorner, viewport.transform);
      pdfjsLib.Util.applyTransform(secondCorner, viewport.transform);

      const link = document.createElement("a");
      link.className = "draft-report-pdf-link";
      link.style.left = `${Math.min(firstCorner[0], secondCorner[0])}px`;
      link.style.top = `${Math.min(firstCorner[1], secondCorner[1])}px`;
      link.style.width = `${Math.abs(firstCorner[0] - secondCorner[0])}px`;
      link.style.height = `${Math.abs(firstCorner[1] - secondCorner[1])}px`;
      link.setAttribute(
        "aria-label",
        annotation.title || annotation.contents || "PDF link",
      );

      if (annotation.url) {
        link.href = annotation.url;
        link.target = "_blank";
        link.rel = "noopener";
      } else {
        link.href = "#";
        link.addEventListener("click", async (event) => {
          event.preventDefault();
          await followInternalDestination(annotation.dest);
        });
      }
      annotationsElement.append(link);
    }
  }

  async function renderPage(number) {
    const state = pageStates.get(number);
    if (!documentHandle || !state || viewportElement.clientWidth < 320) return;

    try {
      const page = await documentHandle.getPage(number);
      const baseViewport = page.getViewport({ scale: 1 });
      state.baseWidth = baseViewport.width;
      state.baseHeight = baseViewport.height;
      const scale = sizePageShell(state);
      if (state.renderedScale === scale) return;

      state.renderTask?.cancel();
      const viewport = page.getViewport({ scale });
      const outputScale = Math.min(window.devicePixelRatio || 1, 2);
      state.canvas.width = Math.floor(viewport.width * outputScale);
      state.canvas.height = Math.floor(viewport.height * outputScale);
      state.canvas.style.width = `${viewport.width}px`;
      state.canvas.style.height = `${viewport.height}px`;

      state.renderTask = page.render({
        canvasContext: state.context,
        viewport,
        transform: outputScale === 1
          ? null
          : [outputScale, 0, 0, outputScale, 0, 0],
      });
      await state.renderTask.promise;

      try {
        await renderAnnotations(page, viewport, state.annotations);
      } catch (annotationError) {
        console.warn(`Unable to add PDF links for page ${number}`, annotationError);
        state.annotations.replaceChildren();
      }

      state.canvas.hidden = false;
      state.renderedScale = scale;
    } catch (error) {
      if (error?.name === "RenderingCancelledException") return;
      console.error(`Unable to render report page ${number}`, error);
    }
  }

  function refreshVisiblePages() {
    for (const number of visiblePages) renderPage(number);
  }

  function refreshLayout() {
    lastViewportWidth = viewportElement.clientWidth;
    for (const state of pageStates.values()) {
      state.renderTask?.cancel();
      state.renderTask = null;
      state.renderedScale = null;
      state.canvas.hidden = true;
      state.annotations.replaceChildren();
      sizePageShell(state);
    }
    refreshVisiblePages();
  }

  function updateCurrentPageFromScroll() {
    scrollFrame = null;
    if (!documentHandle) return;

    const viewportRectangle = viewportElement.getBoundingClientRect();
    const marker = viewportRectangle.top + 24;
    let current = pageNumber;
    let closestDistance = Number.POSITIVE_INFINITY;

    for (const state of pageStates.values()) {
      const rectangle = state.shell.getBoundingClientRect();
      if (rectangle.top <= marker && rectangle.bottom > marker) {
        current = state.number;
        closestDistance = 0;
        break;
      }
      const distance = Math.abs(rectangle.top - marker);
      if (distance < closestDistance) {
        closestDistance = distance;
        current = state.number;
      }
    }

    if (current !== pageNumber) {
      pageNumber = current;
      updateControls();
    }
  }

  async function goToPage(requestedPage, behavior = "smooth") {
    if (!documentHandle) return;
    pageNumber = Math.min(documentHandle.numPages, Math.max(1, requestedPage));
    updateControls();
    const state = pageStates.get(pageNumber);
    if (!state) return;
    await renderPage(pageNumber);
    const pageTop = state.shell.offsetTop - pagesElement.offsetTop;
    viewportElement.scrollTo({
      top: Math.max(0, pageTop),
      behavior,
    });
  }

  function createObservers() {
    renderObserver = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          const number = Number(entry.target.dataset.pageNumber);
          if (entry.isIntersecting) {
            visiblePages.add(number);
            renderPage(number);
          } else {
            visiblePages.delete(number);
            releasePage(number);
          }
        }
      },
      {
        root: viewportElement,
        rootMargin: "100% 0px 100% 0px",
        threshold: 0.01,
      },
    );
    for (const state of pageStates.values()) renderObserver.observe(state.shell);
  }

  previousButton.addEventListener("click", () => goToPage(pageNumber - 1));
  nextButton.addEventListener("click", () => goToPage(pageNumber + 1));
  pageInput.addEventListener("change", () => goToPage(Number(pageInput.value) || 1));
  pageInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      goToPage(Number(pageInput.value) || 1);
    }
  });
  zoomOutButton.addEventListener("click", () => {
    fitWidth = false;
    scaleMultiplier = Math.max(0.5, scaleMultiplier / 1.2);
    refreshLayout();
    goToPage(pageNumber, "auto");
  });
  zoomInButton.addEventListener("click", () => {
    fitWidth = false;
    scaleMultiplier = Math.min(3, scaleMultiplier * 1.2);
    refreshLayout();
    goToPage(pageNumber, "auto");
  });
  fitWidthButton.addEventListener("click", () => {
    fitWidth = true;
    scaleMultiplier = 1;
    refreshLayout();
    goToPage(pageNumber, "auto");
  });
  viewportElement.addEventListener("scroll", () => {
    if (scrollFrame !== null) return;
    scrollFrame = window.requestAnimationFrame(updateCurrentPageFromScroll);
  });
  window.addEventListener("resize", () => {
    if (!documentHandle || !fitWidth) return;
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(refreshLayout, 150);
  });

  const resizeObserver = new ResizeObserver(() => {
    const currentWidth = viewportElement.clientWidth;
    if (
      !documentHandle ||
      !fitWidth ||
      currentWidth < 320 ||
      Math.abs(currentWidth - lastViewportWidth) < 2
    ) return;
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(refreshLayout, 100);
  });
  resizeObserver.observe(viewportElement);

  updateControls();
  pdfjsLib.getDocument({ url: reader.dataset.pdfUrl }).promise
    .then(async (pdfDocument) => {
      documentHandle = pdfDocument;
      pageCountElement.textContent = String(pdfDocument.numPages);
      const firstPage = await pdfDocument.getPage(1);
      const baseViewport = firstPage.getViewport({ scale: 1 });

      for (let number = 1; number <= pdfDocument.numPages; number += 1) {
        createPageState(number, baseViewport);
      }

      pagesElement.hidden = false;
      statusElement.hidden = true;
      lastViewportWidth = viewportElement.clientWidth;
      createObservers();
      updateControls();
      await goToPage(1, "auto");
    })
    .catch((error) => {
      console.error("Unable to load the report PDF", error);
      statusElement.textContent = "The embedded report could not be loaded.";
    });
}
