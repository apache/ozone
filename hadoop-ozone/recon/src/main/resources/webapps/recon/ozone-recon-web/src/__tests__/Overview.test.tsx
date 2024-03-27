import React from "react";
import { BrowserRouter } from "react-router-dom";

/**
 * The dont-cleanup-after-each is imported to prevent autoCleanup of rendered
 * component after each test.
 * Since we are needing to set a timeout everytime the component is rendered
 * and we would be verifying whether the UI is correct, we can skip the cleanup
 * and leave it for after all the tests run - saving test time
 */
import "@testing-library/react/dont-cleanup-after-each";
import { render, screen } from "@testing-library/react";

import { overviewLocators } from "./locators/locators";
import { overviewServer } from "./mocks/overviewMocks/overviewServer";
import { Overview } from "../views/overview/overview";

const WrappedOverviewComponent = () => {
  return (
    <BrowserRouter>
      <Overview />
    </BrowserRouter>
  )
}

describe("Overview Tests", () => {
  beforeAll(async () => {
    overviewServer.listen();
    render(
      <WrappedOverviewComponent />
    );
    //Setting a timeout of 500ms to allow requests to be resolved and states to be set
    await new Promise((r) => { setTimeout(r, 100) })
  });

  afterEach(() => {
    overviewServer.resetHandlers();
  });

  afterAll(() => {
    overviewServer.close();
  })

  // Tests begin here
  // All the data is being mocked by MSW, so we have a fixed data that we can verify
  // the content against
  it("Datanode card has the correct count of Datanodes", () => {
    const datanodeCard = screen.getByTestId(overviewLocators.datanodeCard)
    expect(datanodeCard).toBeVisible();
    expect(datanodeCard).toHaveTextContent("3/5");
  });

  it("Pipelines card has the correct count of Pipelines", () => {
    const pipelinesCard = screen.getByTestId(overviewLocators.pipelinesCard);
    expect(pipelinesCard).toBeVisible();
    expect(pipelinesCard).toHaveTextContent("7");
  });

  it("Capacity card has the correct capacity data", () => {
    const capacityCard = screen.getByTestId(overviewLocators.clusterCapacityCard);
    expect(capacityCard).toBeVisible();
    expect(capacityCard).toHaveTextContent("263.9 GB/1.2 TB");
  });

  it("Volumes card has the correct number of volumes", () => {
    const volumeCard = screen.getByTestId(overviewLocators.volumesCard);
    expect(volumeCard).toBeVisible();
    expect(volumeCard).toHaveTextContent("2");
  });

  it("Buckets card has the correct number of buckets", () => {
    const bucketsCard = screen.getByTestId(overviewLocators.bucketsCard);
    expect(bucketsCard).toBeVisible();
    expect(bucketsCard).toHaveTextContent("24");
  });

  it("Keys card has the correct number of keys", () => {
    const keysCard = screen.getByTestId(overviewLocators.keysCard);
    expect(keysCard).toBeVisible();
    expect(keysCard).toHaveTextContent("1424");
  });

  it("Deleted containers card has the correct number of keys", () => {
    const deletedContainersCard = screen.getByTestId(overviewLocators.deletedContainersCard);
    expect(deletedContainersCard).toBeVisible();
    expect(deletedContainersCard).toHaveTextContent("10");
  });

  it("Open keys summary card has the correct data", () => {
    const openKeysSummaryCard = screen.getByTestId(overviewLocators.openKeysSummaryCard);
    expect(openKeysSummaryCard).toBeVisible();
    expect(openKeysSummaryCard).toHaveTextContent("1 KB Total Replicated Data Size");
    expect(openKeysSummaryCard).toHaveTextContent("4 KB Total UnReplicated Data Size");
    expect(openKeysSummaryCard).toHaveTextContent("10 Total Open KeysOpen Keys Summary");
  });

  it("Pending deleted keys summary card has the correct data", () => {
    const pendingDelKeysCard = screen.getByTestId(overviewLocators.deletePendingSummaryCard);
    expect(pendingDelKeysCard).toBeVisible();
    expect(pendingDelKeysCard).toHaveTextContent("1 KB Total Replicated Data Size");
    expect(pendingDelKeysCard).toHaveTextContent("4 KB Total UnReplicated Data Size");
    expect(pendingDelKeysCard).toHaveTextContent("3 Total Pending Delete KeysPending Deleted Keys Summary");
  });
})