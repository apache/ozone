import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import PipelinesTable from '@/v2/components/tables/pipelinesTable';
import { vi } from 'vitest';
import { Pipeline, PipelinesTableProps } from '@/v2/types/pipelines.types';
import { pipelineLocators } from '@/__tests__/locators/locators';

const defaultProps: PipelinesTableProps = {
  loading: false,
  data: [],
  selectedColumns: [
    { label: 'Pipeline ID', value: 'pipelineId' },
    { label: 'Status', value: 'status' },
  ],
  searchTerm: '',
};

function getPipelineWith(
  id: string,
  status: 'OPEN' | 'CLOSING' | 'QUASI_CLOSED' | 'CLOSED' | 'UNHEALTHY' | 'INVALID' | 'DELETED' | 'DORMANT',
  replicationFactor: string
): Pipeline {
  return {
    pipelineId: id,
    status,
    containers: 10,
    datanodes: [],
    leaderNode: 'node-1',
    replicationFactor,
    replicationType: 'RATIS',
    duration: 10000,
    leaderElections: 1,
    lastLeaderElection: 1000,
  };
}

describe('PipelinesTable Component', () => {
  test('Renders table with data', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[getPipelineWith('pipeline-1', 'UNHEALTHY', 'THREE')]}
      />
    );

    // Verify table is rendered
    await waitFor(() => screen.getByText('pipeline-1'));
    expect(screen.getByText('UNHEALTHY')).toBeInTheDocument();
  });

  test('Filters data based on search term', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        searchTerm="pipeline-1"
        data={[
          getPipelineWith('pipeline-1', 'OPEN', 'THREE'),
          getPipelineWith('pipeline-2', 'QUASI_CLOSED', 'ONE'),
        ]}
      />
    );

    // Verify only matching rows are displayed
    expect(screen.getByText('pipeline-1')).toBeInTheDocument();
    expect(screen.queryByText('pipeline-2')).not.toBeInTheDocument();
  });

  test('Renders correct columns based on selectedColumns', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        selectedColumns={[
          { label: 'Pipeline ID', value: 'pipelineId' },
        ]}
        data={[getPipelineWith('pipeline-1', 'CLOSED', 'ONE')]}
      />
    );

    // Verify only the selected columns are rendered
    expect(screen.getByText('Pipeline ID')).toBeInTheDocument();
    expect(screen.queryByText('Replication Type & Factor')).not.toBeInTheDocument();
    expect(screen.queryByText('Status')).not.toBeInTheDocument();
  });

  test('Sorts table rows by pipeline ID', async () => {
    render(
      <PipelinesTable
        {...defaultProps}
        data={[
          getPipelineWith('pipeline-2', 'CLOSED', 'ONE'),
          getPipelineWith('pipeline-1', 'DELETED', 'THREE'),
        ]}
      />
    );

    // Simulate sorting by pipeline ID
    const pipelineIdHeader = screen.getByText('Pipeline ID');
    fireEvent.click(pipelineIdHeader);

    // Verify rows are sorted
    const rows = screen.getAllByTestId(pipelineLocators.pipelineRowRegex);
    expect(rows[0]).toHaveTextContent('pipeline-1');
    expect(rows[1]).toHaveTextContent('pipeline-2');
  });
});
