package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.hadoop.ozone.recon.schema.ContainerSchemaDefinition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.apache.hadoop.ozone.recon.upgrade.ReconLayoutFeature.UNHEALTHY_CONTAINER_DATA_CHECKSUM;
import static org.apache.hadoop.ozone.recon.upgrade.ReconUpgradeAction.UpgradeActionType.FINALIZE;
import static org.hadoop.ozone.recon.codegen.SqlDbUtils.TABLE_EXISTS_CHECK;
import static org.hadoop.ozone.recon.schema.ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;

/**
 * Upgrade action for handling the addition of a new unhealthy container state in Recon, which will
 */
@UpgradeActionRecon(feature = UNHEALTHY_CONTAINER_DATA_CHECKSUM, type = FINALIZE)
public class UnhealthyContainerDataChecksumAction implements ReconUpgradeAction {
    private static final Logger LOG = LoggerFactory.getLogger(InitialConstraintUpgradeAction.class);
    private DataSource dataSource;
    private DSLContext dslContext;

    @Override
    public void execute(ReconStorageContainerManagerFacade scmFacade) throws Exception {
        this.dataSource = scmFacade.getDataSource();
        try (Connection conn = dataSource.getConnection()) {
            if (!TABLE_EXISTS_CHECK.test(conn, UNHEALTHY_CONTAINERS_TABLE_NAME)) {
                return;
            }
            dslContext = DSL.using(conn);
            // Drop the existing constraint
            dropConstraint();
            // Add the updated constraint with all enum states
            addUpdatedConstraint();
        } catch (SQLException e) {
            throw new SQLException("Failed to execute UnhealthyContainerDataChecksumAction", e);
        }
    }

    /**
     * Drops the existing constraint from the UNHEALTHY_CONTAINERS table.
     */
    private void dropConstraint() {
        String constraintName = UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1";
        dslContext.alterTable(UNHEALTHY_CONTAINERS_TABLE_NAME)
                .dropConstraint(constraintName)
                .execute();
        LOG.debug("Dropped the existing constraint: {}", constraintName);
    }

    /**
     * Adds the updated constraint directly within this class.
     */
    private void addUpdatedConstraint() {
        String[] enumStates = Arrays
                .stream(ContainerSchemaDefinition.UnHealthyContainerStates.values())
                .map(Enum::name)
                .toArray(String[]::new);

        dslContext.alterTable(ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME)
                .add(DSL.constraint(ContainerSchemaDefinition.UNHEALTHY_CONTAINERS_TABLE_NAME + "ck1")
                        .check(field(name("container_state"))
                                .in(enumStates)))
                .execute();

        LOG.info("Added the updated constraint to the UNHEALTHY_CONTAINERS table for enum state values: {}",
                Arrays.toString(enumStates));
    }

    @Override
    public UpgradeActionType getType() {
        return FINALIZE;
    }
}
