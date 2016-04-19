/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.web.api;

import com.sun.jersey.api.core.ResourceContext;
import com.wordnik.swagger.annotations.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.request.ClientIdParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * RESTful endpoint for managing a SiteToSite connection.
 */
@Path("/site-to-site")
@Api(
        value = "/site-to-site",
        description = "Provides SiteToSite API to be called by a SiteToSite client remotely."
)
public class SiteToSiteResource extends ApplicationResource {

    private static final Logger logger = LoggerFactory.getLogger(SiteToSiteResource.class);

    // TODO: Remove serviceFacade if we don't need it.
    private NiFiServiceFacade serviceFacade;
    private WebClusterManager clusterManager;
    private NiFiProperties properties;

    @Context
    private ResourceContext resourceContext;


    /**
     * Returns the details of this NiFi.
     *
     * @param clientId Optional client id. If the client id is not specified, a new one will be generated. This value (whether specified or generated) is included in the response.
     * @return A controllerEntity.
     */
    @GET
    @Path("/peers")
    @Consumes(MediaType.WILDCARD)
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_NIFI')")
    @ApiOperation(
            value = "Returns the details about this NiFi necessary to communicate via site to site",
            response = ControllerEntity.class,
            authorizations = @Authorization(value = "NiFi", type = "ROLE_NIFI")
    )
    @ApiResponses(
            value = {
                @ApiResponse(code = 400, message = "NiFi was unable to complete the request because it was invalid. The request should not be retried without modification."),
                @ApiResponse(code = 401, message = "Client could not be authenticated."),
                @ApiResponse(code = 403, message = "Client is not authorized to make this request."),
                @ApiResponse(code = 409, message = "The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.")
            }
    )
    public Response getPeers(
            @ApiParam(
                    value = "If the client id is not specified, new one will be generated. This value (whether specified or generated) is included in the response.",
                    required = false
            )
            @QueryParam(CLIENT_ID) @DefaultValue(StringUtils.EMPTY) ClientIdParameter clientId) {

        if (properties.isClusterManager()) {
            // TODO: Get peers within this cluster.
            return clusterManager.applyRequest(HttpMethod.GET, getAbsolutePath(), getRequestParameters(true), getHeaders()).getResponse();
        }

        InetSocketAddress apiAddress = properties.getNodeApiAddress();


        RevisionDTO revision = new RevisionDTO();
        revision.setClientId(clientId.getClientId());

        // TODO: Determine if the connection is secured.
        PeerDTO peer = new PeerDTO();
        peer.setHostname(apiAddress.getHostName());
        peer.setPort(apiAddress.getPort());
        peer.setSecure(false);
        peer.setFlowFileCount(0);  // doesn't matter how many FlowFiles we have, because we're the only host.

        ArrayList<PeerDTO> peers = new ArrayList<>(1);
        peers.add(peer);

        PeersEntity entity = new PeersEntity();
        entity.setRevision(revision);
        entity.setPeers(peers);

        return clusterContext(noCache(Response.ok(entity))).build();
    }

    // setters
    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setClusterManager(WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

}
