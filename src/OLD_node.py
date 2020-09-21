from uuid import uuid4

from blockchain import Blockchain
from utility.verification import Verification
from wallet import Wallet


class Node:
    def __init__(self):
        # self.wallet.public_key = str(uuid4())
        self.wallet = Wallet()
        self.wallet.create_keys()
        self.blockchain = Blockchain(self.wallet.public_key)

    def get_transaction_value(self):
        """ Returns the input of the user (a new transaction amount) as a float. """
        tx_recipient = input("Enter the recipient of the transaction: ")
        tx_amount = float(input("Your transaction amount please: "))
        return (tx_recipient, tx_amount)

    def get_user_choice(self):
        """ Gets user choice input """
        user_input = input("Your choice: ")
        return user_input

    def print_blockchain_elements(self):
        """ Output the blockchain list to the console """
        for block in self.blockchain.chain:
            print("Outputting Block")
            print(block)

    def listen_for_input(self):
        waiting_for_input = True
        while waiting_for_input:
            print("")
            print("=" * 20)
            print("Please choose")
            print("1: Add a new transaction value")
            print("2: Mine a new block")
            print("3: Output the blockchain blocks")
            print("4: Check transactions validity")
            print("5: Create wallet")
            print("6: Load wallet")
            print("7: Save keys")
            print("q: Quit")
            user_choice = self.get_user_choice()
            if user_choice == "1":
                tx_data = self.get_transaction_value()
                recipient, amount = tx_data
                signature = self.wallet.sign_transaction(
                    self.wallet.public_key, recipient, amount
                )
                if self.blockchain.add_transaction(
                    recipient, self.wallet.public_key, signature, amount=amount
                ):
                    print("Added transaction!")
                else:
                    print("Transaction failed!")
                print(self.blockchain.get_open_transactions())
            elif user_choice == "2":
                if not self.blockchain.mine_block():
                    print("Mining failed. Got no wallet?")
            elif user_choice == "3":
                self.print_blockchain_elements()
            elif user_choice == "4":
                if Verification.verify_transactions(
                    self.blockchain.get_open_transactions(), self.blockchain.get_balance
                ):
                    print("All transactions are valid")
                else:
                    print("There are invalid transactions")
            elif user_choice == "5":
                self.wallet.create_keys()
                self.blockchain = Blockchain(self.wallet.public_key)
            elif user_choice == "6":
                self.wallet.load_keys()
                self.blockchain = Blockchain(self.wallet.public_key)
            elif user_choice == "7":
                self.wallet.save_keys()
            elif user_choice == "q":
                waiting_for_input = False
            else:
                print("Input was invalid, please pick a value from thej list!")
            if not Verification.verify_chain(self.blockchain.chain):
                self.print_blockchain_elements()
                print("Invalid blockchain hacker!")
                break
            print(
                f"{self.wallet.public_key}'s Balance: {self.blockchain.get_balance():6.2f}"
            )
        else:
            print("Done!")


if __name__ == "__main__":
    node = Node()
    node.listen_for_input()
