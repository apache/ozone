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
  it("Datanode card has the correct count of Datanodes", () => {
    // We are mocking the data, hence we are always expecting 5 datanodes
    // out of which 3 are HEALTHY datanodes
    const datanodeCard = screen.getByTestId("overview-Datanodes")
    expect(datanodeCard).toHaveTextContent("3/5");
  });
  it("Pipelines card has the correct count of Pipelines", () => {
    // We are mocking the data, hence we are always expecting 7 pipelines
    const pipelinesCard = screen.getByTestId("overview-Pipelines");
    expect(pipelinesCard).toHaveTextContent("7");
  });
  // it("Capacity card has the correct capacity data", () => {
  //   const capacityCard = screen.getByTestId("overview-Cluster_Capacity");
  //   expect(capacityCard).toBeVisible();
  //   expect(capacityCard).toHaveTextContent("");
  // });
})