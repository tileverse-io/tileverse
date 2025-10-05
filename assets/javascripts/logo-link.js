// Make logo link to tileverse.io homepage
document.addEventListener('DOMContentLoaded', function() {
  const logo = document.querySelector('.md-header__button.md-logo');
  if (logo) {
    logo.addEventListener('click', function(e) {
      e.preventDefault();
      window.location.href = 'https://tileverse.io';
    });
    // Make it look clickable
    logo.style.cursor = 'pointer';
  }
});