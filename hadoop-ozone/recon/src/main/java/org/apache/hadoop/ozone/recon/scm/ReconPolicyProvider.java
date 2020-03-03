package org.apache.hadoop.ozone.recon.scm;

import static org.apache.hadoop.hdds.recon.ReconConfig.ConfigStrings.OZONE_RECON_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.protocol.ReconDatanodeProtocol;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;

/**
 * {@link PolicyProvider} for Recon protocols.
 */
public final class ReconPolicyProvider extends PolicyProvider {

  private static AtomicReference<ReconPolicyProvider> atomicReference =
      new AtomicReference<>();

  private ReconPolicyProvider() {
  }

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static ReconPolicyProvider getInstance() {
    if (atomicReference.get() == null) {
      atomicReference.compareAndSet(null, new ReconPolicyProvider());
    }
    return atomicReference.get();
  }

  private static final Service[] RECON_SERVICES =
      new Service[]{
          new Service(
              OZONE_RECON_SECURITY_CLIENT_DATANODE_CONTAINER_PROTOCOL_ACL,
              ReconDatanodeProtocol.class)
      };

  @Override
  public Service[] getServices() {
    return RECON_SERVICES;
  }

}
