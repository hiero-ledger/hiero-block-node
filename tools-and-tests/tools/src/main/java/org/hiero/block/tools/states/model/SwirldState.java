// SPDX-License-Identifier: Apache-2.0
package org.hiero.block.tools.states.model;

public record SwirldState() {

    static void init() {
        ServicesState servicesState = new ServicesState();
    }
}
/*
// TODO load state from disk code

					SwirldState state = appMain.newState();
					state.init(this, initialAddressBook.copy()); // hashgraph and state get separate copies of
					// addressBook
					signedState = new SignedState(state);

					SignedStateFileManager.readObjectFromFile(savedStateFiles[i],
							signedState);

					if (Settings.checkSignedStateFromDisk) {
						byte[] hash = signedState.generateSigendStateHash(Crypto.getMessageDigest(), null);
						if (Arrays.equals(hash, signedState.getHash())) {
							log.info(LogMarkers.LOGM_STARTUP, "Signed state loaded from disk has a valid hash.");

							//TODO verify signatures
						} else {
							log.error(LogMarkers.LOGM_STARTUP,
									"ERROR: Signed state loaded from disk has an invalid hash!\ndisk:{}\ncalc:{}",
									Arrays.toString(signedState.getHash()),
									Arrays.toString(hash));
							//TODO change the Exception class to something more specific
							//throw new Exception("Signed state loaded from disk has an invalid hash!");
						}
					}

					// XXX because of FS immutability, the swirld state loaded should be passed to EventFlow
					// this is a hack, and should be removed at some point
					swirldState = signedState.getState();
					signedState.setState((SwirldState) swirldState.copy());

					signedStateMgr
							.addCompleteSignedStateFromDisk(signedState);
					// XXX we will not use the saved address book for now
					// initialAddressBook = signedState.getAddressBook();
					hashgraph.loadFromSignedState(signedState);
 */
