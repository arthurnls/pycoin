from functools import reduce
import hashlib as hl
import json
import pickle
import requests

from utility.hash_util import hash_block
from utility.verification import Verification
from block import Block
from transaction import Transaction
from wallet import Wallet

# The reward we give to miners (for creating a new block)
MINING_REWARD = 10


class Blockchain:
    def __init__(self, public_key, node_id):
        """The constructore of the blockchain class."""
        # Our starting block for the blockchain
        genesis_block = Block(0, "", [], 100, 0)
        # Initializing our empty blockchain list
        self.chain = [genesis_block]
        # Unprocessed transactions
        self.__open_transactions = []
        self.public_key = public_key
        self.__peer_nodes = set()
        self.node_id = node_id
        self.resolve_conflics = False
        self.load_data()

    @property
    def chain(self):
        return self.__chain[:]

    @chain.setter
    def chain(self, val):
        self.__chain = val

    def get_open_transactions(self):
        return self.__open_transactions[:]

    def load_data(self):
        """ Loads current blockchain and open_transactions data from local stored file """
        try:
            with open("data/blockchain-{}.txt".format(self.node_id), mode="r") as f:
                # Using Pickle (must change mode to "rb", file to ".p")
                # file_content = pickle.loads(f.read())
                # blockchain = file_content["chain"]
                # open_transactions = file_content["ot"]
                # Using JSON (must change mode to "r")
                file_content = f.readlines()
                blockchain = json.loads(file_content[0][:-1])
                updated_blockchain = []
                for block in blockchain:
                    converted_tx = [
                        Transaction(
                            tx["sender"], tx["recipient"], tx["signature"], tx["amount"]
                        )
                        for tx in block["transactions"]
                    ]
                    updated_block = Block(
                        block["index"],
                        block["previous_hash"],
                        converted_tx,
                        block["proof"],
                        block["timestamp"],
                    )
                    updated_blockchain.append(updated_block)
                self.chain = updated_blockchain
                open_transactions = json.loads(file_content[1][:-1])
                updated_transactions = []
                for tx in open_transactions:
                    updated_tx = Transaction(
                        tx["sender"],
                        tx["recipient"],
                        tx["signature"],
                        tx["amount"],
                    )
                    updated_transactions.append(updated_tx)
                self.__open_transactions = updated_transactions
                peer_nodes = json.loads(file_content[2])
                self.__peer_nodes = set(peer_nodes)
        except (IOError, IndexError):
            pass

    def save_data(self):
        """ Saves blockchain and open_transactions data into local file """
        try:
            with open("data/blockchain-{}.txt".format(self.node_id), mode="w") as f:
                # Using Pickle (must change mode to "wb", file to ".p")
                # save_data = {"chain": blockchain, "ot": open_transactions}
                # f.write(pickle.dumps(save_data))
                # Using JSON (must change mode to "w")
                saveable_chain = [
                    block.__dict__
                    for block in [
                        Block(
                            block_el.index,
                            block_el.previous_hash,
                            [tx.__dict__ for tx in block_el.transactions],
                            block_el.proof,
                            block_el.timestamp,
                        )
                        for block_el in self.__chain
                    ]
                ]
                f.write(json.dumps(saveable_chain))
                f.write("\n")
                saveable_tx = [tx.__dict__ for tx in self.__open_transactions]
                f.write(json.dumps(saveable_tx))
                f.write("\n")
                f.write(json.dumps(list(self.__peer_nodes)))
        except (IOError, IndexError):
            print("Saving failed!")

    def proof_of_work(self):
        last_block = self.__chain[-1]
        last_hash = hash_block(last_block)
        proof = 0
        while not Verification.valid_proof(self.__open_transactions, last_hash, proof):
            proof += 1
        return proof

    def get_balance(self, sender=None):
        """Calculate and return the balance for a participant"""
        if sender == None:
            if self.public_key == None:
                return None
            participant = self.public_key
        else:
            participant = sender
        # get all transactions from the blockchain where :participant: is the sender
        tx_sender = [
            [tx.amount for tx in block.transactions if tx.sender == participant]
            for block in self.__chain
        ]
        # get all transactions from open_transactions where :participant: is the sender
        open_tx_sender = [
            tx.amount for tx in self.__open_transactions if tx.sender == participant
        ]
        tx_sender.append(open_tx_sender)
        # process the total amount sent
        amount_sent = reduce(
            lambda tx_sum, tx_amt: tx_sum + sum(tx_amt)
            if len(tx_amt) > 0
            else tx_sum + 0,
            tx_sender,
            0,
        )
        # get all transactions from the blockchain where :participant: is the recipient
        tx_recipient = [
            [tx.amount for tx in block.transactions if tx.recipient == participant]
            for block in self.__chain
        ]
        # process the total amount recieved
        amount_received = reduce(
            lambda tx_sum, tx_amt: tx_sum + sum(tx_amt)
            if len(tx_amt) > 0
            else tx_sum + 0,
            tx_recipient,
            0,
        )
        return amount_received - amount_sent

    def get_last_blockchain_value(self):
        """ Returns the last value of the current blockchain """
        if len(self.__chain) < 1:
            return None
        return self.__chain[-1]

    def add_transaction(
        self, recipient, sender, signature, amount=1.0, is_receiving=False
    ):
        """Append a new value as well as the last blockchain value to the blockchain.

        Arguments:
            :sender: The sender of the coins
            :recipient: The recipient of the coins
            :amount: THe amount of coins sent with the transaction (default = 1.0)
        """
        if self.public_key == None:
            return False
        transaction = Transaction(sender, recipient, signature, amount)
        if Verification.verify_transaction(transaction, self.get_balance):
            self.__open_transactions.append(transaction)
            self.save_data()
            if not is_receiving:
                for node in self.__peer_nodes:
                    url = "http://{}/broadcast-transaction".format(node)
                    try:
                        response = requests.post(
                            url,
                            json={
                                "sender": sender,
                                "recipient": recipient,
                                "amount": amount,
                                "signature": signature,
                            },
                        )
                        if response.status_code == 400 or response.status_code == 500:
                            print("Transaction declined, needs resolving")
                            return False
                    except requests.exceptions.ConnectionError:
                        continue
            return True
        return False

    def mine_block(self):
        """ Create a new block and add open transations to it """
        if self.public_key == None:
            return None
        last_block = self.__chain[-1]
        hashed_block = hash_block(last_block)
        proof = self.proof_of_work()
        reward_transaction = Transaction("MINING", self.public_key, "", MINING_REWARD)
        copied_transactions = self.__open_transactions[:]
        for tx in copied_transactions:
            if not Wallet.verify_transaction(tx):
                return None
        copied_transactions.append(reward_transaction)
        block = Block(len(self.__chain), hashed_block, copied_transactions, proof)
        self.__chain.append(block)
        self.__open_transactions = []
        self.save_data()
        for node in self.__peer_nodes:
            url = "http://{}/broadcast-block".format(node)
            try:
                converted_block = block.__dict__.copy()
                converted_block["transactions"] = [
                    tx.__dict__ for tx in converted_block["transactions"]
                ]
                response = requests.post(
                    url,
                    json={
                        "block": converted_block,
                    },
                )
                if response.status_code == 400 or response.status_code == 500:
                    print("Block declined, needs resolving")
                if response.status_code == 409:
                    self.resolve_conflics = True
            except requests.exceptions.ConnectionError:
                continue
        return block

    def add_block(self, block):
        """ Adds a new block to the chain """
        transactions = [
            Transaction(tx["sender"], tx["recipient"], tx["signature"], tx["amount"])
            for tx in block["transactions"]
        ]
        proof_is_valid = Verification.valid_proof(
            transactions[:-1], block["previous_hash"], block["proof"]
        )
        hashes_match = hash_block(self.chain[-1]) == block["previous_hash"]
        if not proof_is_valid or not hashes_match:
            return False
        converted_block = Block(
            block["index"],
            block["previous_hash"],
            transactions,
            block["proof"],
            block["timestamp"],
        )
        self.__chain.append(converted_block)
        stored_transactions = self.__open_transactions[:]
        for itx in block["transactions"]:
            for opentx in stored_transactions:
                if (
                    opentx.sender == itx["sender"]
                    and opentx.recipient == itx["recipient"]
                    and opentx.amount == itx["amount"]
                    and opentx.signature == itx["signature"]
                ):
                    try:
                        self.__open_transactions.remove(opentx)
                    except ValueError:
                        print("Item was already removed.")
        self.save_data()
        return True

    def resolve(self):
        winner_chain = self.chain
        replace = False
        for node in self.__peer_nodes:
            url = "http://{}/chain".format(node)
            try:
                response = requests.get(url)
                node_chain = response.json()
                node_chain = [
                    Block(
                        block["index"],
                        block["previous_hash"],
                        [
                            Transaction(
                                tx["sender"],
                                tx["recipient"],
                                tx["signature"],
                                tx["amount"],
                            )
                            for tx in block["transactions"]
                        ],
                        block["proof"],
                        block["timestamp"],
                    )
                    for block in node_chain
                ]
                node_chain_length = len(node_chain)
                local_chain_length = len(winner_chain)
                if (
                    node_chain_length > local_chain_length
                    and Verification.verify_chain(node_chain)
                ):
                    winner_chain = node_chain
                    replace = True
            except requests.exceptions.ConnectionError:
                continue
        self.resolve_conflics = False
        self.chain = winner_chain
        if replace:
            self.__open_transactions = []
        self.save_data()
        return replace

    def add_peer_node(self, node):
        """Adds a new node to the peer node set.

        Arguments:
            :node: The node URL which should be added.
        """
        self.__peer_nodes.add(node)
        self.save_data()

    def remove_peer_node(self, node):
        """Removes a node from the peer node set.

        Arguments:
            :node: The node URL which should be removed.
        """
        self.__peer_nodes.discard(node)
        self.save_data()

    def get_peer_nodes(self):
        """Return a list of all connected peer nodes."""
        return list(self.__peer_nodes)
