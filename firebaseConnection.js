// Initialize Firebase
var config = {
  apiKey: "AIzaSyAmbS9EKpnJRTE5AAnKKgUFMkdDF7KOePk",
  authDomain: "cruising-23cb7.firebaseapp.com",
  databaseURL: "https://cruising-23cb7.firebaseio.com",
  projectId: "cruising-23cb7",
  storageBucket: "cruising-23cb7.appspot.com",
  messagingSenderId: "322449955301"
};
firebase.initializeApp(config);
var database = firebase.database();

// Initialize Cloud Firestore through Firebase
firebase.initializeApp({
    apiKey: "AIzaSyAmbS9EKpnJRTE5AAnKKgUFMkdDF7KOePk",
    authDomain: "cruising-23cb7.firebaseapp.com",
    projectId: "cruising-23cb7"
  });
  
  var db = firebase.firestore();
function addToDB() {
  // Add a second document with a generated ID.
db.collection("users").add({
    firstName: "Alan",
    lastName: "Turing",
    dob: "07091993",
});
}

// Saves a new message on the Cloud Firestore.
function saveMessage(messageText) {
  // Add a new message entry to the Firebase database.
  return firebase.firestore().collection('messages').add({
    name: getUserName(),
    text: messageText,
    profilePicUrl: getProfilePicUrl(),
    timestamp: firebase.firestore.FieldValue.serverTimestamp()
  }).catch(function(error) {
    console.error('Error writing new message to Firebase Database', error);
  });
}